from hashlib import new
import pyarrow as pa
from pyarrow import compute as pc
from pyarrow import feather, ipc, csv, parquet
from tempfile import TemporaryDirectory
from pathlib import Path
import uuid
import argparse
import random
from typing import List, Set, Dict, Tuple, Optional, Iterator, Union
import logging
import numpy as np
import bisect
import random
import numpy as np
from .utils import total_batch_size, total_batch_nrows, min_val, max_val, tb_min, tb_max, vec_gte, \
    Tablet, get_pivot, consume_tablets, partition, flatten

logger = logging.getLogger("ducksauce")

            
def tmpdata(n = 10000, maxint = 1000, batches = 5):
    tbs = []
    tmpdir = Path("/tmp/tablets")
    for p in tmpdir.glob("*"):
        p.unlink()
    tmpdir.mkdir(exist_ok = True)
    for i in range(batches):
        tb = pa.table({"A": pa.array(np.random.randint(0, maxint, n)),"B": pa.array(np.random.randint(0, maxint, n))})
        min = tb_min(tb, keys = keys)
        max = tb_max(tb, keys = keys)
        p = Tablet(tb, "/tmp/tablets", min, max, keys)
        tbs.append(p)
    return tbs

def subcleave(patab : pa.Table, pivots : List[pa.Scalar], keys):
    key, *keys_left = keys
    pivot, *pivots_left = pivots
    assert pivot.as_py is not None
    
    lesser = pc.filter(patab, pc.less(patab[key], pivot))
    greater = pc.filter(patab, pc.greater(patab[key], pivot))
    eq = pc.filter(patab, pc.equal(patab[key], pivot))
    
    if len(pivots) == 1:
        return lesser, pa.concat_tables([greater, eq])
    
    lt, gt = subcleave(eq, pivots_left, keys_left)
    return (
        pa.concat_tables([lesser, lt]),
        pa.concat_tables([greater, gt])
    )

def cleave(tb : Tablet, pivots : List[pa.Scalar], keys : List[str]):
    """
    breaks a tablet object in two around a single pivot point. Objects exactly
    equal to the pivot will be in the right (greater) table.
    """
    assert isinstance(tb, Tablet)
    lesser, greater = subcleave(tb.table, pivots, keys)
    
    greater = Tablet(greater, tb.path.parent, min = tb_min(greater, keys), max = tb_max(greater, keys), keys = keys)
    lesser = Tablet(lesser, tb.path.parent, min = tb_min(lesser, keys), max = tb_max(lesser, keys), keys = keys)
    if len(greater) == 0:
        greater = None
    if len(lesser) == 0:
        lesser = None
    if lesser is not None:
        assert vec_gte(lesser.max, lesser.min)
    if greater is not None:
        assert vec_gte(greater.max, greater.min)
    tb.destroy()
    return lesser, greater

def split_into(tb : pa.Table, n : int, keys : List[str]) -> List[pa.Table]:
    tbs = [tb]
    for i in range(n):
        tbs.sort(key = lambda x: -len(x))
        current = tbs.pop(0)
        # Pick a random pivot
        if len(current) <= 1:
            # Unlikey limit case.
            tbs.append(current)
            break
        rix = random.randint(0, len(current)-1)    
        pivots = [current[k][rix] for k in keys]
        for part in cleave(current, pivots, keys):
            if part is not None:
                tbs.append(part)
        current.destroy()
    return tbs
        # Sort by length so the next split is on the longest array


def central_pass(inputs : List[Tablet], keys : List[str], max_overlaps : int = 1024 * 1024 * 1024) -> List[List[Tablet]]:
    # Choose a random pivot.
    sorted = []
    unsorted = [inputs]
    i = 0
    while len(unsorted) > 0:
        i += 1
        array = unsorted.pop(0)
        if len(array) == 0:
            continue
        if total_batch_size(array) < max_overlaps:
            array.sort()
            sorted.append(array)
            continue
        size = total_batch_nrows(array)
        pivot = get_pivot(array, keys)
        # Split chunks into three: those below the pivot, those overlapping it, and those above.
        all_before, overlaps, all_after = partition(array, pivot)
        print(f"{i:04d} COMPLETE: {sum(map(total_batch_size, sorted))}, SAFE: {total_batch_size(all_before)}, overlapping: {total_batch_size(overlaps)}, next: {total_batch_size(all_after)}, stack: {sum(map(total_batch_size, unsorted))}", end = "                                             \r")
        assert size == sum(map(total_batch_nrows, [all_before, all_after, overlaps]))
        s1 = []
        s2 = []
        while total_batch_size(overlaps) > max_overlaps:
            current = overlaps.pop()
            before, eq_or_after = cleave(current, pivot, keys)
            s1.append(before)
            s2.append(eq_or_after)

            if total_batch_size(s1) > max_overlaps:
                t = consume_tablets(*s1, keys = keys)
                ts = split_into(t, MINISIZE, keys)
                for t in ts:
                    t.flush()
                    all_before.append(t)
                s1 = []
            if total_batch_size(s2) > max_overlaps:
                t = consume_tablets(*s2, keys = keys)
                ts = split_into(t, MINISIZE, keys)
                s2 = []
                for t in ts:
                    t.flush()
                    all_after.append(t)

                    
        assert size == total_batch_nrows([*s1, *s2, *overlaps, *all_before, *all_after])
        all_before.extend(s1)
        all_after.extend(s2)
        unsorted = [u for u in [all_before + overlaps, all_after, *unsorted] if len(u) > 0]
        
        if sum(map(total_batch_size, unsorted)) <= max_overlaps:
            # The end condition is that the left size is small enough to sort.
            sorted.extend(unsorted)
            break
            
    return sorted


def swansong(sorted_nested : List[List[Tablet]], fout : Path, max_overlaps : int, keys : List[str]) -> None:
    """
    The last portion of the sort is simple--just move left to 
    right sorting each portion in turn, secure in the knowledge 
    that you won't run out.
    """
    if fout.suffix == ".feather":
        writer = ipc.new_file(fout, sorted_nested[0][0].schema)
    elif fout.suffix == ".parquet":
        writer = parquet.ParquetWriter(fout, sorted_nested[0][0].schema)
    stack = []
    sorted : List[Tablet] = flatten(sorted_nested)
    i = 0
    while len(sorted) > 0:
        i += 1
        tb = sorted.pop(0)
        stack.append(tb)
        if len(sorted) == 0 or total_batch_size(stack + [sorted[0]]) > max_overlaps:
            combined = consume_tablets(*stack, keys = keys)
            stack = []
            if len(sorted) > 0:
                min_remaining = min_val(*sorted)
                lesser, greater = cleave(combined, pivots = min_remaining, keys = keys)
                if greater is not None and len(greater) > 0:
                    stack = [greater]
            else:
                lesser = combined
            ixes = pc.sort_indices(lesser.table, sort_keys = [(k, "ascending") for k in keys])
            lesser = pc.take(lesser.table, ixes)
            writer.write_table(lesser)
            print(f"{i:04d}: Flushed {lesser.nbytes} bytes with {total_batch_size(stack)} remaining in cache")
                
    writer.close()


def yield_from_csv(input, block_size):
    f = csv.open_csv(input, read_options = csv.ReadOptions(block_size = block_size),
            parse_options = csv.ParseOptions(delimiter = ","))
    yield from f

def yield_from_feather(path):
    logger.debug(f"Reading file from {path}")
    inp = ipc.open_file(path)
    for i in range(inp.num_record_batches):
        yield inp.get_batch(i)
    
def yield_from_parquet(path):
    fin = parquet.ParquetFile(path)
    yield from fin.iter_batches()

def yield_from_file(path):
    if path.suffix == ".parquet":
        yield from yield_from_parquet(path)
    elif path.suffix == ".feather":
        yield from yield_from_feather(path)
    elif path.suffix == ".csv":
        yield from yield_from_csv(path)
    else:
        raise FileNotFoundError("Huh?", path)

def from_files(files, keys: List[str], output: Union[Path, str], block_size = 2_500_000_000):
    output = Path(output)
    assert not output.exists()
    def iterator():
        for path in files:
            path = Path(path)
            assert path.exists()
            yield from yield_from_file(path)
    quacksort(iterator(), keys, output, block_size)
    
def __main__():
    args = parse_args()
    print(args)
    from_files(args.inputs, args.key, args.output, args.block_size)

def parse_args():
    parser = argparse.ArgumentParser(description='Sort some files.')
    parser.add_argument('--key', type=str, action="append",
                        help='The names of the columns to sort on.')
    parser.add_argument('--block-size', type=int,
                        default = 2_500_000_000,
                        help="The maximum size of tables to hold in memory, in bytes. Performance "
                        "depends on making this as big as possible. Default 2_500_000_000 (2.5 gigabytes)")                        
    parser.add_argument('inputs', nargs = '+', metavar='file', type=Path, help = "The files to sort.")
    parser.add_argument('output', type=Path, help = "The file to write into. Type will be gleaned from suffix--must be '.parquet' or '.feather'")
    return parser.parse_args()


def ducksauce(input, **args):
  """

  """
  if input.suffix == ".csv":
      from_csv(input, **args)
  if input.suffix == ".feather":
      from_feather(input, **args)

MINISIZE = 12
def pass_1(iterator : Iterator[pa.RecordBatch], keys: List[str], block_size : int, tmpdir : Path):
    tables = []
    cache = []
    logger.info("Reading initial stream for quacksort.")
    n_records = 0
    for i, batch in enumerate(iterator):
        n_records += len(batch)
        cache.append(batch)
        if total_batch_size(cache) > block_size:
            block = pa.Table.from_batches(cache)
            min = tb_min(block, keys = keys)
            max = tb_max(block, keys = keys)
            tab = Tablet(block, tmpdir, min, max, keys)            
            array = split_into(tab, MINISIZE, keys)
            for subbatch in array:
                tables.append(subbatch)
                # Write it to disk and clear the memory version.
                subbatch.flush()
            cache = []
            cache_size = 0
    # Flush the cache at the end.
    if len(cache) > 0:
        block = pa.Table.from_batches(cache)
        min = tb_min(block, keys = keys)
        max = tb_max(block, keys = keys)
        tab = Tablet(block, tmpdir, min, max, keys)            
        array = split_into(tab, MINISIZE, keys)
        for subbatch in array:
            tables.append(subbatch)
            # Write it to disk and clear the memory version.
            subbatch.flush()
    assert len(tables) > 0
    assert(n_records == sum([len(f) for f in tables]))
    return tables


def quacksort(iterator: Iterator[pa.RecordBatch], keys: List[str], output: Union[Path, str], block_size = 2_500_000_000) -> None:
    
    """
    Some kind of multi-pass sorting algorithm that aims to reduce useless ahead-
    of time sorting. 

    iterator: something that yields an iterator over arrow recordbatches.
    keys: an ordered list of sort keys.
    output: the destination file for a parquet file.
    block_size: the block size in bytes. I wouldn't be shocked if 
    actual memory consumption doubles this on occasion. 
    """

    output = Path(output)
    n_records = 0
    # First pass--simply write to disk.
    tables : List[Tablet] = []
    with TemporaryDirectory(dir = ".") as tmp_dir:
        chunked_tables = pass_1(iterator, keys, block_size, Path(tmp_dir))
        # The central pass chunks into half the block size.
        mostly_sorted = central_pass(chunked_tables, keys, block_size / 2)
        swansong(mostly_sorted, Path(output), block_size, keys)


if __name__=="__main__":
    __main__()
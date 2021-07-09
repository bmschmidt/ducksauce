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
logger = logging.getLogger("ducksauce")

def split(batch, key):
    """
    Partition (not sort) a table into front half, back half.
    Time complexity is greatly reduced by using partition rather than sort.
    
    Memory usage here is unclear to me. 
    """
    if len(batch) < 3:
        return [batch]
    batch = batch.combine_chunks()
    mid = random.randint(1, len(batch) - 1)
    mid = len(batch) // 2
    batch.take([mid])
    try:
        sort_order = pc.partition_nth_indices(batch[key], options=pc.PartitionNthOptions(mid))
    except:
        logger.error("Couldn't partition", mid, batch[key])
        raise
    front = batch.take(sort_order[0:mid])
    end = batch.take(sort_order[mid:])
    return [front, end]

def partition(batch, key, times = 4):
    """
    times: number of splits. Power of 2.
    
    Split an array into parts.
    """
    array = [batch]
    for _ in range(2**times):
        first = array.pop(0)
        new_items = split(first, key)
        array += new_items
        del first
    return array

def is_ordered(new_files, batch_size):
    """
    Pass over the memorized files to see if the batch
    size could conceivable write a lower to number to
    disk before a higher number appears.
    """
    queue = []
    buff_size = 0
    for (min, max, size, _) in new_files:
        queue.append((min, max, size))
        buff_size += size
        if buff_size > batch_size:
            _, f_max, f_size = queue.pop(0)
            if f_max > min:
                return False
            buff_size -= f_size
        
    return True


# Better Key: pc.add(tb['_ncid'], pc.multiply(tb['wordid'], pc.add(1, pc.min_max(tb['_ncid'])['max'])))

def yield_from_csv(input, block_size):
    f = csv.open_csv(input, read_options = csv.ReadOptions(block_size = block_size),
            parse_options = csv.ParseOptions(delimiter = ","))
    yield from f

def yield_from_feather(path):
    print("Feathering", path)
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
    assert not output.exists()
    def iterator():
        for path in files:
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
    parser.add_argument('--block-size', type=int, nargs=1,
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

class MyTable():
    def __init__(self, table, dir, key):
        self.path = Path(dir) / (str(uuid.uuid1()) + ".feather")
        self.minmax = None
        self.set_min_max(table, key)
        if self.minmax['min'] is None:
            raise("foo")
        self.nbytes = table.nbytes
        pa.feather.write_feather(table, self.path)
        self.length = len(table)
        self._table = None

    @property 
    def table(self):
        if self._table:
            return self._table
        self._table = feather.read_table(self.path, memory_map = True)
        return self._table
    
    def __getattr__(self, key):
        return self.table.__getattr__(key)
    
    def __getitem__(self, key):
        return self.table.__getitem__(key)
        
    def set_min_max(self, table, key):
        if pa.types.is_integer(table[key].type):
            self.minmax = pc.min_max(table[key]).as_py()
        else:
            # E.g. string, binary.
            arr = table[key]
            # Get the min and max the hard way.
            min = arr.take(pc.partition_nth_indices(arr, pivot = 0))
            max = arr.take(pc.partition_nth_indices(arr, pivot = len(arr) - 1))
            # Don't cast to python in case collation would be different.
            self.minmax = {
                "min": min,
                "max": max
            }

    def destroy(self):
        self.path.unlink()

def quacksort(iterator: Iterator[pa.RecordBatch], keys: List[str], output: Union[Path, str], block_size = 2_500_000_000):
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
    cache_size = 0
    total_bytes = 0
    cache = []
    tables = []
    """
    First pass--chunk into files of 1/8 the block size.
    """ 
    key = keys[0]
    n_written = 0
    with TemporaryDirectory() as tmp_dir:
        logger.info("Reading initial stream for quacksort.")
        for i, batch in enumerate(iterator):
            n_records += len(batch)
            cache_size += batch.nbytes
            total_bytes += batch.nbytes
            cache.append(batch)        
            if cache_size > block_size:
                block = pa.Table.from_batches(cache)
                array = partition(block, key, 3)
                for subbatch in array:
                    tables.append(MyTable(subbatch, tmp_dir, key))
                    n_written += 1
                logger.debug(n_written, f"batches written to {tmp_dir}")
                cache = []
                cache_size = 0
        # Flush the cache
        if len(cache) > 0:
            block = pa.Table.from_batches(cache)
            array = partition(block, key, 2)
            for subbatch in array:
                tables.append(MyTable(subbatch, tmp_dir, key))

        assert(n_records == sum([f.length for f in tables]))
        
        n_splits = 3
        logger.info("Initial stream read: preparing shuffle sort.")
        while True:
            tables.sort(key = lambda x: x.minmax['min'])
            malordered = malordered_ranges(tables, block_size)
            if len(malordered) == 0:
                break
            score = np.sum(np.array([m[0] for m in malordered]))
            # Randomness avoids getting stuck... but at what cost?
            worst, start, end, description = random.choice(malordered)
            logger.warning(f"{score} bad, reordering {worst:.0f} from {start} to {end} " + description + "     ")
            head = tables[:start]
            to_fix = tables[start:end]
            tail = tables[end:]
            if len(to_fix) == 0:
                logger.warning("to_fix has length 0")
                to_fix = [tables[start]]
                tail = tables[start + 1:]
            reorder_table = pa.concat_tables([f.table for f in to_fix])
            new_parts = partition(reorder_table, key, n_splits)
            new_mid = [MyTable(subbatch, tmp_dir, key) for subbatch in new_parts]
            # Cleanup.
            for f in to_fix:
                try:
                    f.destroy()
                except FileNotFoundError:
                    "WTF?"
                    raise
                    continue
            tables = head + new_mid + tail
        cache_size = 0
        cache = []
        out_num = 0
        written = 0
        logger.info("Finishing sort.")
        final_outfile = None
        for i, tab in enumerate(tables):
            cache.append(tab.table)
            cache_size += tab.nbytes
            tab.destroy()
            try:
                next_min = tables[i + 1].minmax['min']
            except IndexError:
                # Max 32-byte float. will need adjustment if ever want to do this on 64 bits.
                next_min = None
            if cache_size >= block_size or next_min is None:
                if final_outfile is None:
                    if output.suffix == ".feather":
                        final_outfile = ipc.new_file(output, schema = cache[0].schema)
                    if output.suffix == ".parquet":
                        final_outfile = parquet.ParquetWriter(output, schema=cache[0].schema)
                tab = pa.concat_tables(cache)
                sort_order = pc.sort_indices(tab[key])
                tab = tab.take(sort_order)
                out_num += 1
                if next_min is not None:
                    mask = pc.less(tab[key], pa.scalar(next_min, pa.int64()))
                    done = tab.filter(mask)
                else:
                    done = tab
                if output.suffix == ".feather":
                    for record_batch in done.to_batches():
                        final_outfile.write_batch(record_batch)
                elif output.suffix == ".parquet":
                    final_outfile.write_table(done)
                written += done.nbytes
                if next_min is not None:
                    leftover = tab.filter(pc.invert(mask))

                # The cache are values that might be part of the next item.
                cache = [leftover]
                cache_size = leftover.nbytes
        # No need for a final flush
        final_outfile.close()
        logger.debug("Sort done.")

import bisect

def malordered_ranges(files, batch_size):
    """
    Pass over the memorized files to see if the batch
    size could conceivable write a lower to number to
    disk before a higher number appears.
    """
    queue = []
    buff_size = 0
    info = []

    byte_counts = np.array([f.nbytes for f in files])

    mins = [f.minmax['min'] for f in files]
    maxes = [f.minmax['max'] for f in files]
    max_to_the_left = 0
    for i, f in enumerate(files):
        right_min = f.minmax['min']
        right_max = f.minmax['max']
        right_size = f.nbytes
        queue.append((right_min, right_max, right_size, i))
        buff_size += f.nbytes
        if buff_size > batch_size:
            # The leftmost element
            left_min, left_max, left_size, left_i = queue.pop(0)
            max_to_the_left = max(left_max, max_to_the_left)
            # The first file we could safely write out to the left has a maximum value
            # less than right_min.
            might_overlap_left = right_min
            overlap_left = bisect.bisect_left(maxes, might_overlap_left)

            
            # # The first file we could safely write out to the right
            # has a minimum value greater than left_max, or the highest 
            # already-dropped maximum. (The assymetry is because of how we sort.)
            might_overlap_right = max_to_the_left
            overlap_right = bisect.bisect_right(mins, might_overlap_right)

            # A metric: how many bytes outside this buffer might overlap?
            extraneousness = (np.sum(byte_counts[overlap_left:overlap_right + 1]) - buff_size)
            # If it's zero, we could sort in one pass.
            if extraneousness > 0:
                info.append((extraneousness, left_i, i, f"{overlap_left}<- {left_i} - {i + 1} ->{overlap_right} ({extraneousness:.01f})"))
            buff_size -= left_size

    info.sort(reverse = True)
    return info


if __name__=="__main__":
    __main__()
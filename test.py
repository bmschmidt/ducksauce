from ducksauce import from_files
from pathlib import Path
#from random import randint
from numpy import random
import pyarrow as pa
from pyarrow import feather
from pyarrow import ipc, compute as pc
r = 1_000_000
N = 10_000_000
block_size = 100_000


def test(rr, N, block_size):
    def r():
        return random.randint(0, rr, N // 20)

    def batches():
        return pa.table({
            'A': pa.array(r(), pa.uint32()),
            'B': pa.array(r(), pa.uint32()),
            'C': pa.array(r(), pa.uint32())
        }).to_batches()
    b = batches()
    print(f"Generating {N} random integers")
    p = Path("test.feather")
    if p.exists():
        p.unlink()
    with ipc.new_file(p, schema = batches()[0].schema) as output:
      for i in range(20):
        for b in batches():
          output.write_batch(b)
      output.close()

    from pyarrow import compute
    output = Path("sorted.feather")
    if output.exists():
        output.unlink()
    from_files([p], keys = ["C", "A"], output = output, block_size = block_size)

    read = feather.read_table(output)
    any_mistakes = pc.any(pc.less(read['C'][1:], read['C'][:-1])).as_py()
    assert not any_mistakes
    assert len(read) == N
    p.unlink()

test(1_000_000, 10_000_000, 50_000)
#test(1_000_000, 5_000_000, 10_000)
#test(1_000_000, 10_000, 10_000)
#test(1_000_000, 10_000, 100_000)

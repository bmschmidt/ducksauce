import pytest
from ducksauce.ducksauce import pass_1, quacksort, from_files, swansong
from ducksauce.utils import Tablet, tb_min, tb_max, flatten
from pathlib import Path
#from random import randint

from numpy import random
import pyarrow as pa
from pyarrow import feather
from pyarrow import ipc, compute as pc, parquet
import uuid
import numpy as np
import numpy.random
r = 1_000_000
N = 10_000_000
block_size = 100_000

def tmpdata(dir, n = 10000, maxint = 1000, batches = 5):
    tbs = []
    tmpdir = Path(dir)
    tmpdir.mkdir(exist_ok = True)
    keys = ["A", "B", "C"]
    for i in range(batches):
        tb = pa.table({
          "A": pa.array(np.random.randint(0, maxint, n)),
          "B": pa.array(np.random.randint(0, maxint, n)),
          "C": pa.array(np.random.randint(0, maxint, n))
          })

        min = tb_min(tb, keys = keys)
        max = tb_max(tb, keys = keys)
        p = Tablet(tb, dir, min, max, keys)
        tbs.append(p)
    return tbs
  
@pytest.fixture(scope="module")
def char_batches():
    # Letters: low of "A", high of "z"
    chars = [chr(a) for a in random.randint(low = 65, high = 123, size = 100_000)]
    dummy = ["A" for i in range(len(chars))]
    tab = pa.table({
      'dummy': pa.array(dummy, type=pa.string()),
      'key': pa.array(chars, type=pa.string()),
      'n': pa.array(np.arange(len(chars)), type=pa.int64())})
    return tab.to_batches(block_size = 1_000)



class TestInitialSort():
  def test_character_pass_1(self, char_batches, tmp_path):
    pass_1(char_batches, ["key"], 10_000, tmp_path)
  def test_character_pass_1_with_dummy(self, char_batches, tmp_path):
    pass_1(char_batches, ["dummy", "key"], 10_000, tmp_path)

class Test_Safety():
  def test_pass_1_length(self, tmp_path):
    N = 10_000
    tablets = tmpdata(dir=tmp_path, n = 10_000, maxint = 1000, batches = 1)
    batches = [t.to_batches() for t in tablets]
    batches = flatten(batches)
    assert len(batches) > 0
    g = pass_1(batches, keys = ['A', 'B', 'C'], block_size = 1000, tmpdir = Path(tmp_path))
    assert sum([len(g) for g in g]) == N

class Test_Tokens():
  def test_token_sort(self, tmp_path):
    root = tmp_path
    chars = [str(a) for a in random.randint(0, 1000, 1000)]
    tab = pa.table({
      'key': pa.array(chars, type=pa.string()),
      'n': pa.array(np.arange(len(chars)), type=pa.int64())})
    parquet.write_table(tab, root / "test.parquet")
    output = root / 'test2.parquet'
    from_files([root / 'test.parquet'], keys = ['key'], output = output, block_size = 500)
    t = parquet.read_table(root / 'test2.parquet')['key'].to_pylist()
    for a, b in zip(t[:-1], t[1:]):
      assert a <= b


  def test_dummy_double_sort(self, tmp_path):
    """
    Given a dummy column in the first position, is the second column sorted?
    """
    chars = [str(a) for a in random.randint(low = 0, high = 1000, size = 10000)]
    dummy = ["A" for i in range(len(chars))]
    tab = pa.table({
      'dummy': pa.array(dummy, pa.string()),
      'key': pa.array(chars, type=pa.string()),
      'n': pa.array(np.arange(len(chars)), type=pa.int64())})
    parquet.write_table(tab, tmp_path / "test.parquet")
    output = Path(tmp_path / 'test2.parquet')
    from_files([tmp_path / 'test.parquet'], keys = ['dummy', 'key'], output = output, block_size = 1000000)
    t = parquet.read_table(tmp_path / 'test2.parquet')
    indices = pc.sort_indices(t, sort_keys = [('dummy', 'ascending'),
      ('key', 'ascending')])
    # Does it align with a normal sort?
    assert indices[:10].to_pylist() == [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

  def test_true_double_sort(self, tmp_path):
    """
    Given a dummy column in the first position, is the second column sorted?
    """
    A = [str(a) for a in random.randint(low = 0, high = 1000, size = 100_000)]
    B = [str(a) for a in random.randint(low = 0, high = 1000, size = 100_000)]

    tab = pa.table({
      'A': pa.array(A, type=pa.string()),
      'B': pa.array(B, type=pa.string()),
      'n': pa.array(np.arange(len(A)), type=pa.int64())})

    parquet.write_table(tab, tmp_path / "test.parquet")
    output = Path(tmp_path / 'test2.parquet')
    from_files([tmp_path / 'test.parquet'], keys = ['A', 'B'], output = output, block_size = 50_000)
    t = parquet.read_table(output)
    indices = pc.sort_indices(t, sort_keys = [('A', 'ascending'),
      ('B', 'ascending')])
    # Does it align with a normal sort?
    assert indices[:10].to_pylist() == [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
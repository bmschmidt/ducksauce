import pytest
import ducksauce
from ducksauce import from_files

from pathlib import Path
#from random import randint
from numpy import random
import pyarrow as pa
from pyarrow import feather
from pyarrow import ipc, compute as pc, parquet
import uuid
import numpy as np
r = 1_000_000
N = 10_000_000
block_size = 100_000


class Test_Tokens():
  def test_token_sort(self):

    chars = [str(a) for a in random.randint(0, 1000, 10000)]
    tab = pa.table({
      'key': pa.array(chars, type=pa.string()),
      'n': pa.array(np.arange(len(chars)), type=pa.int64())})
    parquet.write_table(tab, "/tmp/test.parquet")
    output = Path('/tmp/test2.parquet')
    if output.exists():
      output.unlink()
    from_files(['/tmp/test.parquet'], keys = ['key'], output = output, block_size = 1000)
  

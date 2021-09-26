import pyarrow as pa
from pyarrow import feather, parquet, ipc
from pathlib import Path
import numpy as np
from numpy.random import choice
import argparse
import logging

logger = logging.getLogger("ducksauce")
logger.setLevel(level = logging.INFO)
def parse_args():
  parser = argparse.ArgumentParser(description='Merge shuffled files, maintaining shuffle.')
  parser.add_argument('--block-size', type=int,
                      default = 2_500_000_000,
                      help="The maximum size of tables to hold in memory, in bytes. Performance "
                      "depends on making this as big as possible. Default 2_500_000_000 (2.5 gigabytes)")                        
  parser.add_argument('--indexed', action='store_true', help = "Whether to index the output.", default = True)
  parser.add_argument('inputs', nargs = '+', metavar='file', type=Path, help = "The files to sort.")
  parser.add_argument('output', type=Path, help = "The file to write into. Type will be gleaned from suffix--must be '.parquet' or '.feather'")
  return parser.parse_args()


def pass_once(tbs, pass_num, num_passes):
  portions = []
  for tb in tbs:
    stride = (len(tb) + 1) // num_passes
    start = pass_num * stride
    end = min(start + stride, len(tb))
    portions.append(tb.take(np.arange(start, end)))
  unshuffled = pa.concat_tables(portions)
  shuflist = choice(len(unshuffled), len(unshuffled), replace = False)
  return unshuffled.take(shuflist)

def main():
  args = parse_args()
  if args.output.suffix == '.parquet':
    raise NotImplementedError("Parquet output not implemented yet")
  tbs = []
  total : int = 0
  for path in args.inputs:
    assert path.suffix in ('.parquet', '.feather')
    if path.suffix == '.parquet':
      raise NotImplementedError("Parquet input not implemented yet")
    tb = feather.read_table(path)
    tbs.append(tb)
    total += tb.nbytes
    logger.info(f"Read {path}")
  num_passes = (total // args.block_size) + 1
  schema = tbs[0].schema
  if args.indexed:
    schema = schema.append(pa.field('ix', pa.uint32()))
    ix_num = 0
  output = ipc.new_file(args.output, schema = schema)
  for i in range(num_passes):
    logger.info(f"Path {i} of {num_passes}")
    result = pass_once(tbs, i, num_passes)
    if args.indexed:
      result = result.append_column("ix", pa.array(np.arange(ix_num, ix_num + len(result)), pa.uint32()))
      ix_num += len(result)
    output.write_table(result)
  output.close()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
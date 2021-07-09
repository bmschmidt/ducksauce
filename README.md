# Duck Sauce

There comes a time in every coder's life when they have to write a sort routine. For most, that happens when they're 18 and being hazed by an introductory CS course. For me, it's apparently now. Bundling now because I keep encountering 
these cases where I need to sort multi-gigabyte feather/parquet files that can't fit into memory, and this is a good enough way to do it.


## Command line

ducksauce --key size --input 0.parquet 1.parquet [etc] output.feather


This is designed for a very particular use case. You start with an iterator over pyarrow record batches (I've build in 
some automatic methods to generate these from CSV, Parquet, or Arrow), and write to a single massive file (feather or parquet formatted.) Intermediate on-disk files are generated and then deleted. It takes advantage of the pyarrow.compute `partition_nth_indices` function which is a lot faster than fully sorting sublists for a traditional merge sort.

I don't know if this is really better than any of the other ways of doing this. I don't even know whether this algorithm even works in all cases--the worst-case scenario, when the sort buffer is extremely small compared to the dataset size, is certainly quite bad. But it seems to work fine (i.e.; a couple passes) as long as the memory buffer is at least 0.1% the size of the dataset, which seems to be true in most cases I've seen. 

So if the buffer is 2.5 GB (the default), this should be OK on 2.5TB of data, hopefully.

Currently only implemented for sorting on unsigned integer fields, because that's what I needed. And a limited number of them, at that. (No fixed limited, but if the range for set 1 is A = 16, the range for set 2 is B = 100,000, etc., this routing will fail if A * B * C * ... * Z > 2**64.


# Strategy

The sort here is kind of like a merge sort, I think? It goes in three stages:

1. Ingest. Stream blocks in, and when enough have accumulated in memory, partition (not sort) them to disk in 8 chunks.
      > At the end of this stage, there should be something like 8 different blocks.
2. Roving sort. Keep a list of all files in memory, with a record of the highest and lowest values in each. Find a contiguous
   batch that shares the most overlap; sort that batch out; repeat. Roving sort continues untile there are no files with overlaps
   that aren't next to each other on the sorted list.
3. Final sort. Go through the stack of files in memory, and read them in in turn. Periodically flush and sort the items
   that are guaranteed to be lower in value than the next unread file.

In steps 2 and 3, the derived files created by steps 1. and 2. are destroyed as they're used, so the on-disk footprint is rarely more than 110% or so the final output size.


# Why "Duck Sauce?"

It's the secret sauce that makes Bookworm in DuckDB with partial BRIN indices faster than MySQL
with full-on B-Tree indices. What, do you want me to call it "worm sauce?" That's gross.

Quacksort is the crown jewel here, but maybe there will be some other algorithm as the need appears.
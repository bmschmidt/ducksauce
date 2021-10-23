# Duck Sauce

There comes a time in every coder's life when they have to write a sort routine. For most, that happens when they're 18 and being hazed by an introductory CS course. For me, it's apparently now. Bundling now because I keep encountering 
these cases where I need to sort multi-gigabyte feather/parquet files that can't fit into memory, and this is a good enough way to do it.



## Command line

quacksort --key size --input 0.parquet 1.parquet [etc] output.feather


This is designed for a very particular use case. You start with an iterator over pyarrow record batches (I've build in 
some automatic methods to generate these from CSV, Parquet, or Arrow), and write to a single massive file (feather or parquet formatted.) Intermediate on-disk files are generated and then deleted. It takes advantage of the pyarrow.compute `partition_nth_indices` function which is a lot faster than fully sorting sublists for a traditional merge sort.

I don't know if this is really better than any of the other ways of doing this. I don't even know whether this algorithm even works in all cases--the worst-case scenario, when the sort buffer is extremely small compared to the dataset size, is certainly quite bad. But it seems to work fine (i.e.; a couple passes) as long as the memory buffer is at least 0.1% the size of the dataset, which seems to be true in most cases I've seen. 

So if the buffer is 2.5 GB (the default), this should be OK on at least 2.5TB of data, hopefully.

# Strategy

The sort here is kind of like a merge sort, I think? It goes in three stages:

1. Ingest. Stream blocks in, and when enough have accumulated in memory, partition (not sort) them to disk around random pivots. After this is done, there
   will be a few dozen large feather files in a temporary directory.
2. Partial sort. Using a quicksort-like pivot approach, read and re-partition overlapping chunks to ensure that for point in the list do a number of files more than `buffer_size` need
   to be in memory at the same time to ensure that the list is sorted.
3. Final sort. Go through the stack of files in memory, and read them in in turn. Periodically flush and sort the items
   that are guaranteed to be lower in value than the next unread file.

In steps 2 and 3, the derived files created by steps 1. and 2. are destroyed as they're used, so the on-disk footprint is rarely more than 110% or so the final output size.

## Why "quacksort"?

Ducks quack, I wanted to work with duckDB, and the sort algorithm for arranging and subdividing chunks is quicksort like.

## Did you rewrite the sorting algorithm entirely to be more quicksort like *after* naming it?

I'm going to take the fifth on that.

# Why "Duck Sauce?"

It's the secret sauce that makes Bookworm in DuckDB with partial BRIN indices faster than MySQL
with full-on B-Tree indices. What, do you want me to call it "worm sauce?" That's gross.

Quacksort is the crown jewel here, but maybe there will be some other algorithm as the need appears.
For instance, there's another tool in here to merge shuffled lists together in a way that preserves their shuffled-ness.

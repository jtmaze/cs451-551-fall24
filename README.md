# CS 451/551: Database Processing, Fall 2024

Acknowledgments and Thanks to Prof. Mohammad Sadoghi (UC Davis)

---

## Team Gnocchi
* Kaegan Koski
* James Maze
* William Qiu
* Eric Zander

Implementation of LStore.

---

## Durability & Bufferpool Extension

A directory is created based on the path supplied to db.open.

Here, some metadata and pages are stored. The temporary page subdirectory
will contain copies made by a background thread during merges and subsequently
copied over to the main page by the main thread.

Pages can be dirty and pinned, but the latter attribute is more in anticipation
of milestone 3; not used for much now.

Pages are evicted from memory to the disk, by default according to LRU
eviction. Both this and the number of maximum pages in memory can be altered
in ./lstore/config.py

---

## Merging

**Note on the GIL and multithreading vs multiprocessing**

Due to python's global interpreter lock, multiple threads must be run 
synchronously and only provide tangible benefits in the context of I/O bound 
tasks.

However, particularly when inciting scripts lack the 'if __name__ == "__main__"',
construct, multiprocessing threatens to execute on a per-import basis; there
are ways around this (ex. using a worker), but due to this and the other
challenges  inherent to multiprocessing, we did not do it for this milestone.

Instead, we use multithreading despite its drawbacks. While this is functional
and theoretically supports minimal time saved during the file I/O
tasks required during the merge, the general lack of asynchronicity largely
just leads to the merge process slowing down transactions.

**Tuning the merge**

For this reason, we set the merge threshold relatively high in config.py.
This describes the number of updates made before a merge is triggered. 

Should this number be lower in your test scripts than the currently set
threshold, you can change the MERGE_UPDATE_THRESHOLD value in ./lstore/config.py
to verify that the merge is functional.

---

## Indexing

Secondary indexes are created on all non-primary key columns by default.

While this supports querying on all columns, this may not be desirable for
performance. If you would like to specify the columns you need an index for,
you can create an IndexConfig object and pass it to db.create_table as below.

A index will be made on the primary key regardless of whether it is specified.

```
from lstore.index_types.index_config import IndexConfig

config = IndexConfig(index_columns=[0, 3])

grades_table = db.create_table('Grades', 5, 0, config)
```

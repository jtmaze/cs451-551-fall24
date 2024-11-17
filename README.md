# CS 451/551: Database Processing, Fall 2024

Acknowledgments and Thanks to Prof. Mohammad Sadoghi (UC Davis)

## Team Gnocchi
* Kaegan Koski
* James Maze
* William Qiu
* Eric Zander

Implementation of LStore.

---

## Durability & Bufferpool Extension

(persistence)

---

## Merging

### Note on the GIL and multithreading vs multiprocessing

Due to python's global interpreter lock, multiple threads must be run 
synchronously and only provide tangible benefits in the context of I/O bound 
tasks.

However, particularly when inciting scripts lack the 'if __name__ == "__main__"',
construct, multiprocessing threatens to execute on a per-import basis; there
are ways around this (ex. using a worker), but due to this and the other
challenges with inherent to multiprocessing, we did not do it for
this milestone.

Instead, we use multithreading despite its drawbacks. While this is functional
and theoretically supports time saved during the relatively minimal file I/O
tasks required during the merge, the general lack of asynchronicity largely
leads to the merge process slowing down transactions.

### Tuning the merge

For this reason, we set the merge threshold relatively high in config.py. Should
the merge not initialize in your test scripts, you can change the 
MERGE_UPDATE_THRESHOLD singleton object in ./lstore/config.py to verify that the
merge is functional.

---

## Indexing

(secondary indexes)

---

## Additional Unit Tests (low level milestone #1 opperations)

If you would like to explore some of our own unit tests, which are very similar to the milestone #1 test scripts. The tests are not especially compelling, but if you're interested... open your terminal, 1. pip install pytest and 2. run pytest (ie $ pytest). 




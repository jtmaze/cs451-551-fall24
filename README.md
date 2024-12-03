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

## Final Submission Highlights

### **Durability & Bufferpool Enhancements**

The database now ensures **durability** through robust disk I/O mechanisms:  

- **Directory Management:**  
  A directory is created at the specified path upon calling `db.open`. This directory stores metadata, pages, and temporary files required for background operations.  
  - Temporary files are used during merge operations and are seamlessly moved to the main directory upon completion.

- **Bufferpool Enhancements:**  
  - Pages can now be marked as **dirty** or **pinned**. Pinned pages are protected from eviction.  
  - **Eviction Policy:** The bufferpool evicts pages using an **LRU (Least Recently Used)** strategy by default.  
  - Both the eviction policy and maximum number of in-memory pages are configurable via `./lstore/config.py`.

---

### **Merging**

#### **Threading vs. Multiprocessing**  

- Due to Python's **Global Interpreter Lock (GIL)**, we opted for **multithreading** instead of multiprocessing for the merge process.  
  - While threading supports minimal performance improvements during I/O-bound tasks, it ensures safe execution and avoids complexities like per-import execution issues in multiprocessing.  
  - This ensures stability, but the merge process may slightly delay transactions.

#### **Merge Threshold**  

- To balance transaction performance and merge frequency, the **MERGE_UPDATE_THRESHOLD** is set relatively high in `./lstore/config.py`.  
- You can adjust this value to test merge functionality under different workloads. Lowering the threshold will trigger merges more frequently, allowing for easier verification of merge behavior.

---

### **Indexing**

- By default, **secondary indexes** are created on all non-primary key columns.  
  - This ensures support for fast querying on all columns but may impact performance when unnecessary indexes are created.  

- To customize indexing:  
  - Use an `IndexConfig` object to specify the columns you need indexed.  
  - The primary key column is always indexed by default.

**Example: Configuring Specific Indexes**  

```python
from lstore.index_types.index_config import IndexConfig

# Create a configuration to index columns 0 (primary) and 3 only
config = IndexConfig(index_columns=[0, 3])

# Create table with custom index configuration
grades_table = db.create_table('Grades', 5, 0, config)

# CS 451/551: Database Processing, Fall 2024

Acknowledgments and Thanks to Prof. Mohammad Sadoghi (UC Davis)

---

## Team Gnocchi
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
```

### **Transactions & Logging**
Transactions adhere to ACID (Atomicity, Consistency, Isolation, Durability) principles:

#### **Features**
- **Atomicity:**
  - All queries within a transaction succeed or are rolled back completely in case of failure.

- **Rollback Support:**
  -Updates and deletes are logged to enable rollback during transaction failure.
  -Update logs store original column values, while delete logs store full records for restoration.

- **Logging Mechanism:**
  - Logs are maintained during the transaction lifecycle and cleared automatically upon successful commit.

- **Transaction Worker:**
  - Enables concurrent execution of multiple transactions using multithreading.
  - Tracks the number of committed transactions and provides statistics for debugging or optimization.

**Example Usage**
```python
from lstore.transaction import Transaction
from lstore.query import Query

# Begin a transaction
transaction = Transaction()

# Add a query
transaction.add_query(query.update, grades_table, primary_key, *updated_columns)

# Run the transaction
if not transaction.run():
    print("Transaction failed and rolled back.")
else:
    print("Transaction committed successfully.")
```

### **Testing & Validation**
- Comprehensive unittests are provided to verify core functionalities, including:
  - Table creation, insertion, selection, updating, and deletion.
  - Merge operations and bufferpool eviction. 
  - Index reconstruction and transaction rollback scenarios.
- Tests cover edge cases such as:
  - Duplicate primary key insertion.
  - Out-of-range queries.
  - Transaction conflicts.

### **Final Notes**
This submission represents a robust and functional implementation of LStore, balancing functionality, performance, and modularity. Configuration parameters can be easily adjusted to suit various testing environments and workloads.

**We hope this system demonstrates our understanding of database design principles and our ability to implement them in practice.**
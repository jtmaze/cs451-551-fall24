import time
import threading

from lstore.storage.thread_local import ThreadLocalSingleton

class Transaction:
    _ts_lock = threading.Lock()
    _last_ts = 0

    def __init__(self):
        """
        # Creates a transaction object.
        """
        self.queries = []
        self.state = "active"  # Transaction state: active, committed, aborted
        self.insert_logs = []  # Store logs for rollback
        self.update_logs = []  # Log for rollback of updates

        self.ts = self._next_timestamp()

    def add_query(self, query, table, *args):
        """
        # Adds the given query to this transaction
        # Example:
        # q = Query(grades_table)
        # t = Transaction()
        # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
        """
        self.queries.append((query, table, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, table, args in self.queries:
            # Log changes for rollback
            if query.__name__ == "insert":
                self.insert_logs.append((query, table, args))

                result = query(*args)
                
                # Check insert worked
                primary_key = args[0]
                rids = table.index.locate(table.key, primary_key)
                if not rids:
                    return self.abort()
            elif query.__name__ in ("update", "delete"):
                self.update_logs.append((query, table, args))

                result = query(*args)

                # Check update/delete worked
                if result is False:
                    return self.abort()
                
            # If transaction caused an issue with an earlier transaction, rollback
            if self.state == "abort":
                return self.abort()
            
        return self.commit()

    def abort(self):
        """
        Rolls back the transaction by undoing all changes.
        """
        self.state = "aborted"
        print("Transaction aborted. Rolling back changes...")

        # Rollback insertions
        for query, table, args in reversed(self.insert_logs):
            primary_key = args[0]
            table.rollback_insert(primary_key)

        # Rollback updates
        for query, table, args in reversed(self.update_logs):
            primary_key = args[0]
            table.rollback_update(primary_key)

        self.insert_logs.clear()
        self.update_logs.clear() 

        thread_local = ThreadLocalSingleton.get_instance()
        for lock in reversed(thread_local.held_locks):
            lock.release()

        return False

    def commit(self):
        """
        Finalizes the transaction and commits changes to the database.
        """
        self.state = "committed"
        print("Transaction committed successfully.")
        # Clear logs since changes are committed
        self.insert_logs.clear()
        self.update_logs.clear()
        return True
    
    @classmethod
    def _next_timestamp(cls):
        with cls._ts_lock:
            ts = time.monotonic_ns()
            while ts <= cls._last_ts:
                ts = cls._last_ts + 1
            cls._last_ts = ts
            return ts

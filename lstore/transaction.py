import time

from lstore.storage.thread_local import ThreadLocalSingleton

class Transaction:

    def __init__(self):
        """
        # Creates a transaction object.
        """
        self.queries = []
        self.state = "active"  # Transaction state: active, committed, aborted
        self.insert_logs = []  # Store logs for rollback
        self.update_logs = []  # Log for rollback of updates

        self.ts = time.time()


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
            # If transaction caused an issue with an early transaction, rollback
            if self.state == "abort":
                self.abort()

            result = query(*args)
            
            # If the query fails, the transaction should abort
            if result is False:
                return self.abort()
            
            # Log changes for rollback
            if query.__name__ == "insert":
                self.insert_logs.append((query, table, args))
            if query.__name__ in ("update", "delete"):
                self.update_logs.append((query, table, args))
            
        return self.commit()

    def abort(self):
        """
        Rolls back the transaction by undoing all changes.
        """
        self.state = "aborted"
        print("Transaction aborted. Rolling back changes...")

        # Rollback insertions
        for query, table, args in reversed(self.insert_logs):
            pass

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

    def log_update(self, rid, original_columns):
        """
        Logs an update operation for rollback.
        :param rid: The RID of the record being updated.
        :param original_columns: The original column values before the update.
        """
        self.update_logs.append((rid, original_columns))

    def log_delete(self, rid, record):
        """
        Logs a delete operation for rollback.
        :param rid: The RID of the record being deleted.
        :param record: The full record being deleted.
        """
        self.delete_logs.append((rid, record))

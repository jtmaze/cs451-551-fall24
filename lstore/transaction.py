from lstore import table
from lstore.table import Table, Record
from lstore.index import Index


class Transaction:
    """
    # Creates a transaction object.
    """

    def __init__(self):
        self.queries = []
        self.logs = []  # Store logs for rollback
        self.state = "active"  # Transaction state: active, committed, aborted
        self.update_logs = []  # Log for rollback of updates
        self.delete_logs = []  # Log for rollback of deletes

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))
        # Log changes for rollback
        if query.__name__ == "update" or query.__name__ == "delete":
            self.logs.append((query, table, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, table, args in self.queries:
            try:
                result = query(*args)
                # If the query fails, the transaction should abort
                if result is False:
                    return self.abort()
            except Exception as e:
                print(f"Transaction failed due to error: {e}")
                return self.abort()
        return self.commit()

    """
    Rolls back the transaction by undoing all changes.
    """

    def abort(self):
        self.state = "aborted"
        print("Transaction aborted. Rolling back changes...")

        # Rollback updates
        for rid, original_columns in reversed(self.update_logs):
            table.rollback_update(rid, original_columns)

        # Rollback deletes
        for rid, record in reversed(self.delete_logs):
            table.rollback_delete(rid)

        self.update_logs.clear()
        self.delete_logs.clear()
        return False

    def commit(self):
        """
        Finalizes the transaction and commits changes to the database.
        """
        self.state = "committed"
        print("Transaction committed successfully.")
        # Clear logs since changes are committed
        self.update_logs.clear()
        self.delete_logs.clear()
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
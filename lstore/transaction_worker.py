import time
import threading

from collections import OrderedDict

from lstore.transaction import Transaction
from lstore.storage.thread_local import ThreadLocalSingleton
from lstore.storage.thread_lock import RollbackCurrentTransaction, RollbackOtherTransaction


class TransactionWorker:
    """
    # Creates a transaction worker object.
    """
    key_locks = dict()
    key_locks_lock = threading.Lock()

    def __init__(self, transactions=None):
        self._thread_local = ThreadLocalSingleton.get_instance()

        self.stats = []
        if transactions is None:
            self.transactions = []
        else:
            self.transactions = transactions

        self.priority_queue = []

        self.result = 0
        self.thread = None  # Thread for running transactions

        self.acquired_locks = []

        self.rid_to_key = dict()

    def add_transaction(self, t):
        """
        Appends t to transactions
        """
        self.transactions.append(t)


    def run(self):
        """
        Runs all transaction as a thread
        """
        # Start a thread to execute transactions
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()


    def join(self):
        """
        Waits for the worker to finish
        """
        if self.thread:
            self.thread.join()

    def __run(self):
        for transaction in self.transactions:
            try:
                self._acquire_locks(transaction)
                self.stats.append(transaction.run())
            finally:
                self._release_locks()

        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))


    # Helpers -------------------------

    def _acquire_locks(self, transaction: Transaction, retries=100):
        all_keys = []

        for query, table, args in transaction.queries:
            key = args[0]

            if query.__name__ in ("insert", "update", "delete"):
                all_keys.append(key)

                rid = self._get_rid_from_primary_key(table, key)
                self.rid_to_key[rid] = key
            elif query.__name__ in ("select", "select_version"):
                col_idx = args[1]

                if col_idx != table.key:
                    rids = table.index.locate(col_idx, args[0])
                    for rid in rids:
                        if rid in self.rid_to_key:
                            all_keys.append(self.rid_to_key[rid])
                else:
                    all_keys.append(key)

        all_keys = sorted(all_keys)

        for _ in range(retries):
            acquired_locks = []
            try:
                # Check if all locks are available
                if any(
                    key in self.key_locks and not self.key_locks[key].acquire(timeout=0)
                    for key in all_keys
                ):
                    raise RollbackCurrentTransaction("Locks unavailable, retrying...")
                
                # Acquire all locks
                for key in all_keys:
                    lock = self._create_lock(key)
                    if not lock.acquire(timeout=0.1):
                        raise RollbackCurrentTransaction("Timeout while acquiring lock...")
                    acquired_locks.append(lock)

                # If successful, add locks to acquired_locks
                self.acquired_locks.extend(acquired_locks)
                return 
            except RollbackCurrentTransaction:
                # Release acquired locks on failure and retry
                print("retrying...")
                for lock in acquired_locks:
                    lock.release()
                time.sleep(0.1)  # Wait before retrying

    def _release_locks(self):
        for lock in reversed(self.acquired_locks):
            lock.release()
        self.acquired_locks.clear()

    @classmethod
    def _create_lock(cls, rid):
        if rid is not None:
            if rid not in cls.key_locks:
                cls.key_locks[rid] = threading.Lock()

            return cls.key_locks[rid]
        
        return None

    def _get_rid_from_primary_key(self, table, primary_key):
        rids = table.index.locate(table.key, primary_key)
        if rids:
            return rids[0]
        
        return None
            
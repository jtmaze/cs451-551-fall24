import time
import threading

from lstore.storage.thread_local import ThreadLocalSingleton
from lstore.storage.thread_lock import RollbackCurrentTransaction, RollbackOtherTransaction


class TransactionWorker:
    """
    # Creates a transaction worker object.
    """
    _thread_local = ThreadLocalSingleton.get_instance()

    def __init__(self, transactions=None):
        self.stats = []
        if transactions is None:
            self.transactions = []
        else:
            self.transactions = transactions

        self.priority_queue = []

        self.result = 0
        self.thread = None  # Thread for running transactions

    """
    Appends t to transactions
    """

    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs all transaction as a thread
    """

    def run(self):
        # Start a thread to execute transactions
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()

    """
    Waits for the worker to finish
    """

    def join(self):
        if self.thread:
            self.thread.join()

    def __run(self):
        for transaction in self.transactions:
            # Save transactions timestamp in thread's local storage
            self._thread_local.transaction = transaction

            # each transaction returns True if committed or False if aborted
            self._run_transaction(transaction)

            # If conflicts caused transactions to be pushed to queue, do them now
            for priority_transaction in self.priority_queue:
                self._run_transaction(priority_transaction)
            self.priority_queue = []

            # Release all locks acquired during transaction
            for lock in self._thread_local.held_locks:
                lock.release()

        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))


    # Helpers -------------------------

    def _run_transaction(self, transaction):
        try:
            self.stats.append(transaction.run())
        except RollbackCurrentTransaction as error:
            other_transaction = error.other_transaction

            transaction.abort()

            while other_transaction.state != "commited":
                time.sleep(0.1)

            self.priority_queue.append(transaction)
        except RollbackOtherTransaction as error:
            other_transaction = error.transaction

            other_transaction.state = "abort"

            self.priority_queue.append(other_transaction)
            
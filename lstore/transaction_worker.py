import time
import threading

from lstore.transaction import Transaction
from lstore.storage.thread_local import ThreadLocalSingleton
from lstore.storage.thread_lock import RollbackCurrentTransaction, RollbackOtherTransaction


class TransactionWorker:
    """
    # Creates a transaction worker object.
    """
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
            # Save transactions timestamp in thread's local storage
            self._thread_local.transaction = transaction

            # each transaction returns True if committed or False if aborted
            self._run_transaction(transaction)

            # If conflicts caused transactions to be pushed to queue, do them now
            while self.priority_queue:
                priority_transaction = self.priority_queue.pop()
                priority_transaction.state = "active"
                self._run_transaction(priority_transaction)
            self.priority_queue.clear()

            # Release all locks acquired during transaction
            for lock in self._thread_local.held_locks:
                lock.release()

        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))


    # Helpers -------------------------

    def _run_transaction(self, transaction: Transaction):
        try:
            self.stats.append(transaction.run())
        except RollbackCurrentTransaction as error:
            other_transaction = error.other_transaction

            transaction.abort()

            while other_transaction.state != "committed":
                time.sleep(0.1)

            self.priority_queue.append(transaction)
        except RollbackOtherTransaction as error:
            other_transaction = error.other_transaction

            other_transaction.state = "abort"
            
            self.priority_queue.append(other_transaction)
            
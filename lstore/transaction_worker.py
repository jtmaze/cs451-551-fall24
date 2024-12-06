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
            self.stats.append(transaction.run())

        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

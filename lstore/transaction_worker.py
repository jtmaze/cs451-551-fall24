import threading


class TransactionWorker:
    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions=None):
        self.stats = []
        if transactions is None:
            self.transactions = []
        else:
            self.transactions = transactions

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
            self.stats.append(transaction.run())

        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

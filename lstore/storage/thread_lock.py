import time

import threading

from lstore.storage.thread_local import ThreadLocalSingleton

class RollbackCurrentTransaction(Exception):
    def __init__(self, ot) -> None:
        self.other_transaction = ot

class RollbackOtherTransaction(Exception):
    def __init__(self, ot) -> None:
        self.other_transaction = ot

class ThreadLock:
    def __init__(self) -> None:
        self._thread_local = ThreadLocalSingleton.get_instance()

        self.lock = threading.Lock()

        self.transaction = None

    def acquire(self):
        """
        Acquires lock.
        
        Return None if successful, or timestamp of the transaction that
        currently has the lock if not.
        """
        # Prevent double acquisition by the same thread by checking list
        if self in self._thread_local.held_locks:
            return True
        
        # Acquire lock
        acquired = self.lock.acquire(blocking=False)
        if acquired:
            self._thread_local.held_locks.append(self)
            self.transaction = self._thread_local.transaction
            return False

        # If failed, compare current thread's time to lock's transaction's time
        current_ts = self._thread_local.transaction.ts
        if current_ts > self.transaction.ts:
            raise RollbackCurrentTransaction(self.transaction)
        elif current_ts < self.transaction.ts:
            raise RollbackOtherTransaction(self.transaction)
        
        raise RuntimeError("Lock unable to be acquired by same thread.")

    def release(self):
        self.lock.release()
        if self in self._thread_local.held_locks:
            self._thread_local.held_locks.remove(self)

        self.transaction = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    @classmethod
    def get_thread_locks(cls):
        singleton = ThreadLocalSingleton.get_instance()
        return singleton.held_locks

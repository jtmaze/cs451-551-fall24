import threading

class ThreadLocalSingleton:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        # Ensure only one instance is ever created globally
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._thread_local = threading.local()
        return cls._instance

    @property
    def held_locks(self):
        if not hasattr(self._thread_local, "held_locks"):
            self._thread_local.held_locks = []
        return self._thread_local.held_locks

    @held_locks.setter
    def held_locks(self, value):
        if not hasattr(self._thread_local, "held_locks"):
            self._thread_local.held_locks = []
        self._thread_local.held_locks = value

    @property
    def transaction(self):
        if not hasattr(self._thread_local, "transaction"):
            self._thread_local.transaction = None
        return self._thread_local.transaction

    @transaction.setter
    def transaction(self, value):
        if not hasattr(self._thread_local, "transaction"):
            self._thread_local.transaction = None
        self._thread_local.transaction = value

    @classmethod
    def get_instance(cls):
        return cls()

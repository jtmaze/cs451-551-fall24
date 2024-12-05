import threading

class ThreadLocalSingleton:
    _thread_local = threading.local()

    def __init__(self):
        # Prevent creating multiple instances in the same thread
        if hasattr(self._thread_local, "instance"):
            raise RuntimeError("Use get_instance() to get the singleton instance.")
            
    @classmethod
    def init_thread_local(cls):
        if not hasattr(cls._thread_local, "held_locks"):
            cls._thread_local.held_locks = []
        if not hasattr(cls._thread_local, "transaction"):
            cls._thread_local.transaction = None

    @property
    def held_locks(self):
        return self._thread_local.held_locks
    
    @held_locks.setter
    def held_locks(self, value):
        self._thread_local.held_locks = value

    @property
    def transaction(self):
        return self._thread_local.transaction
    
    @transaction.setter
    def transaction(self, value):
        self._thread_local.transaction = value

    @classmethod
    def get_instance(cls):
        if not hasattr(cls._thread_local, "instance"):
            cls._thread_local.instance = cls()
        return cls._thread_local.instance

    
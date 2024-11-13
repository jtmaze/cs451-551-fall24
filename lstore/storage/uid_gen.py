import os
import json
import threading


class UIDGenerator:
    """
    Persistent UID generator.

    Creates and reads counter from a file.
    """

    def __init__(self, name, file_dir, uid_bits, batch_size=100_000):
        self.name = name

        os.makedirs(file_dir, exist_ok=True)

        self.file_path = os.path.join(file_dir, f"{name}_gen.json")
        self.max_val = (2 ** uid_bits) - 1
        self.batch_size = batch_size

        self.lock = threading.Lock()

        self._load_last_uid()

    def next_uid(self):
        """
        Generate the next UID.

        If the current batch is exhausted, load a new batch.
        """
        with self.lock:
            if self.current < self.batch_end:
                # Load a new batch
                self.batch_end = max(self.current - self.batch_size, 0)
                self.last_uid = self.batch_end
                self._save_last_uid()

            if self.current < 0:
                raise ValueError(f"No more UIDs for '{self.name}' available")

            # Return the next UID in the batch
            next_uid = self.current
            self.current -= 1

            return next_uid

    # Helpers ------------

    def _load_last_uid(self):
        """Load the last used UID and initialize the batch."""
        if os.path.exists(self.file_path):
            with open(self.file_path, "r") as f:
                data = json.load(f)
                self.last_uid = data.get("last_uid", self.max_val)
        else:
            self.last_uid = self.max_val

        # Setup to load batch right away
        self.current = self.last_uid
        self.batch_end = self.current + 1

    def _save_last_uid(self):
        """Save the last used UID to the file."""
        temp_path = f"{self.file_path}.tmp"
        try:
            with open(temp_path, "w") as f:
                json.dump({"last_uid": self.last_uid}, f)
            os.replace(temp_path, self.file_path)  # Atomic rename
        except IOError as e:
            raise ValueError(f"Failed to save UID data to '{self.file_path}': {e}")

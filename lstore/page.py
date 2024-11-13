from lstore import config
from lstore.storage.page_manager import PageManager

page_manager = PageManager()

class Page:

    def __init__(self, page_id = None):
        self.id = page_id if page_id is not None else page_manager.generate_page_id()
        self.num_records = 0
        self.data = bytearray(config.PAGE_SIZE)
        self.is_dirty = False       # Dirty flag
        self.pin_count = 0          # Pin count

    def has_capacity(self):
        record_size = config.RECORD_SIZE # Cache to skip namespace lookups

        return len(self.data) - self.num_records * record_size >= record_size

    def write(self, value):
        if self.has_capacity():
            record_size = config.RECORD_SIZE # Cache to skip namespace lookups

            start_index = self.num_records * record_size
            self.data[start_index:start_index + record_size] = value.to_bytes(record_size, byteorder='big', signed=True)
            self.num_records += 1
        else:
            # If the page is full, raise an exception
            raise Exception("Page is full. Cannot write more records.")

        # Returning the offset where the value was written
        return start_index
        
    def read(self, offset):
        """
        Reads a value at the given offset.
        :param offset: The byte offset where the value starts.
        :returns: The integer value read from the offset.
        """
        # Extract the bytes from the page's data starting at the offset
        value_bytes = self.data[offset:offset + config.RECORD_SIZE]
        # Convert the extracted bytes to an integer and return it
        return int.from_bytes(value_bytes, byteorder='big', signed=True)
    
    def update(self, val, offset):
        """
        Updates the value at the given offset.
        :param val: The new value to be written.
        :param offset: The byte offset where the value should be updated.
        """
        record_size = config.RECORD_SIZE # Cache to skip namespace lookups

        # Convert the new value to bytes and overwrite the old data
        self.data[offset:offset + record_size] = val.to_bytes(record_size, byteorder='big', signed=True)
        
    def invalidate(self, rid):
        # TODO: Page 'deletion'
        raise NotImplementedError()
    
    def get_all_page_rids(self):
        """
        Returns a list of all RIDs in the page.
        Method woud be useful in the merge_mgr.py, but maybe it should be in disk.py?
        """
        
        pass
    


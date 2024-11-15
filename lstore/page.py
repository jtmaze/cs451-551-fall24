from lstore import config
from lstore.storage.page_manager import PageManager

page_manager = PageManager()

class Page:
    record_size = config.RECORD_SIZE
    page_size = config.PAGE_SIZE

    # Number of values in page header (currently: num_records)
    num_header_records = 1
    
    max_records = (page_size // (record_size * 8)) - num_header_records

    def __init__(self, page_id):
        self.id = page_id
        self.data = bytearray(Page.page_size)

        # Use property setter to set number of records
        self.num_records = 0

        self.is_dirty = False       # Dirty flag
        self.pin_count = 0          # Pin count
        
    def has_capacity(self):
        return self.num_records < Page.max_records

    def write(self, value):
        num_records = self.num_records # Cache result

        if num_records < Page.max_records:
            record_size = Page.record_size # Cache to skip namespace lookups

            # Num records header + filled slots
            start_index = record_size + num_records * record_size

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
        value_bytes = self.data[offset:offset + Page.record_size]
        # Convert the extracted bytes to an integer and return it
        return int.from_bytes(value_bytes, byteorder='big', signed=True)
    
    def update(self, val, offset):
        """
        Updates the value at the given offset.
        :param val: The new value to be written.
        :param offset: The byte offset where the value should be updated.
        """
        record_size = Page.record_size # Cache to skip namespace lookups

        # Convert the new value to bytes and overwrite the old data
        self.data[offset:offset + record_size] = val.to_bytes(record_size, byteorder='big', signed=True)
        
    def invalidate(self, rid):
        # TODO: Page 'deletion'
        raise NotImplementedError()

from lstore import config

class Page:

    def __init__(self, id):
        self.id = id
        self.num_records = 0
        self.data = bytearray(config.PAGE_SIZE)

    def has_capacity(self):
        return len(self.data) - self.num_records * config.RECORD_SIZE >= config.RECORD_SIZE

    def write(self, value):
        if self.has_capacity():
            start_index = self.num_records * config.RECORD_SIZE
            self.data[start_index:start_index + config.RECORD_SIZE] = value.to_bytes(config.RECORD_SIZE, byteorder='big')
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
        return int.from_bytes(value_bytes, byteorder='big')
    
    def update(self, val, offset):
        """
        Updates the value at the given offset.
        :param val: The new value to be written.
        :param offset: The byte offset where the value should be updated.
        """
        # Convert the new value to bytes and overwrite the old data
        self.data[offset:offset + config.RECORD_SIZE] = val.to_bytes(config.RECORD_SIZE, byteorder='big')
        
    def invalidate(self, rid):
        # TODO: Page 'deletion'
        raise NotImplementedError()
    


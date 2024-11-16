from lstore import config

class Page:
    record_size = config.RECORD_SIZE # In bytes
    page_size = config.PAGE_SIZE     # 4096 bytes

    header_size = record_size * 1  # Currently: (num_records)

    def __init__(self, page_id):
        self.id = page_id
        self.data = bytearray(Page.page_size)

        self.offset = Page.header_size  # Start after header

        self.is_dirty = False       # Dirty flag
        self.pin_count = 0          # Pin count

    @classmethod
    def from_data(cls, data, page_id):
        page = cls(page_id)

        page.offset = int.from_bytes(
            data[:Page.record_size], byteorder="big", signed=True)

        page.data = bytearray(data)

        return page
    
    def __iter__(self):
        """
        Generator that allows iteration through contents of page
        """
        record_size = Page.record_size
        
        start_index = Page.header_size
        end_index = Page.header_size + record_size

        # While less than the number of filled bytes in the page
        while end_index <= self.offset:
            yield int.from_bytes(
                self.data[start_index:end_index], byteorder="big", signed=True)

            start_index = end_index
            end_index = end_index + record_size
    
    def write(self, value):
        # Cache vals
        offset = self.offset
        record_size = Page.record_size

        start_index = offset
        end_index = offset + record_size

        if end_index <= Page.page_size:
            # Num records header + filled slots
            self.data[start_index:end_index] = value.to_bytes(
                record_size, byteorder='big', signed=True)

            self._set_offset(end_index)
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
        self.data[offset:offset + record_size] = val.to_bytes(
            record_size, byteorder='big', signed=True)
        
    # Helpers ------------------

    def _read_offset(self):
        return int.from_bytes(
            self.data[:Page.record_size], byteorder="big", signed=True)

    def _set_offset(self, value):
        self.offset = value
        self.data[:Page.record_size] = value.to_bytes(
            Page.record_size, byteorder="big", signed=True)

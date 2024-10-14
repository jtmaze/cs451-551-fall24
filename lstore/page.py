import config

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(config.PAGE_SIZE)

    def has_capacity(self):
        record_size = 256
        return len(self.data) - self.num_records * record_size >= record_size

    def write(self, value):
        record_size = 256
        start_index = self.num_records * record_size
        self.data[start_index:start_index + record_size] = value.to_bytes(record_size, byteorder='big')
        self.num_records += 1
        else:
        # If the page is full, raise an exception
        raise Exception("Page is full. Cannot write more records.")


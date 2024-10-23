import config

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
        
        # TODO: Return offset
        return None
        
    def read(self, offset):
        raise NotImplementedError()
    
    def update(self, val, offset):
        raise NotImplementedError()
        
    def invalidate(self, rid):
        # TODO: Page 'deletion'
        raise NotImplementedError()
    


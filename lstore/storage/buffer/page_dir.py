from collections import OrderedDict

from lstore import config

class PageDirectory:
    def __init__(self, table) -> None:
        self.table = table

        self.capacity = config.MAX_BUFFER_PAGES
        self.size = 0

    def insert(self, rid, record_idx):
        self.base[rid] = record_idx

        if len(self.base) >= self.capacity:
            pass
    

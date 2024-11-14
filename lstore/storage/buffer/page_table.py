from collections import OrderedDict

from lstore import config

from lstore.page import Page
from lstore.storage.uid_gen import UIDGenerator
from lstore.storage.rid import RID

class PageTableEntry:
    def __init__(self, pages_id, tcols, is_base) -> None:
        self.data = [Page(pages_id, is_base) for _ in range(tcols)]

        self.id = pages_id
        
        self.bytes = 0
        self.next_size = config.RECORD_SIZE

        self.size = tcols

    def get_loc(self):
        return self.id, self.bytes

    def __getitem__(self, index):
        return self.data[index]
    
    def __setitem__(self, index, value):
        self.data[index] = value

    def __len__(self):
        return len(self.data)
    
    def has_capacity(self):
        return self.next_size <= config.PAGE_SIZE
    
    def write_vals(self, cols):
        for col, page in enumerate(self.data):
            page.write(cols[col])
            page.is_dirty = True

        self.bytes += config.RECORD_SIZE
        self.next_size += config.RECORD_SIZE

    def pop_page(self, col):
        page = self[col]

        self.col = None

        # Update size and delete entry if empty
        self.size -= 1
        if self.size <= 0:
            del self[col]

        return page


class PageTable:
    pages_id_gen = UIDGenerator("pages_id", config.UID_DIR, RID.pages_id_bits, 10_000)

    def __init__(self, tcols) -> None:
        self.ptable = OrderedDict()
        
        self.tcols = tcols
        self.size = 0

    def __iter__(self):
        return iter(self.ptable)
    
    def __getitem__(self, key):
        return self.ptable[key]

    def move_to_end(self, pages_id, last):
        self.ptable.move_to_end(pages_id, last)

    def create_pages(self, is_base):
        pages_id = PageTable.pages_id_gen.next_uid()

        pages = PageTableEntry(pages_id, self.tcols, is_base)

        self.ptable[pages_id] = pages

        self.size += self.tcols

        return pages, pages_id

    def get_pages(self, pages_id):
        return self.ptable.get(pages_id, None)

    def get_page(self, pages_id, col):
        try:
            return self.ptable[pages_id][col]
        except KeyError:
            return None

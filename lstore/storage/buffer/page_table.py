from collections import OrderedDict

from lstore import config

from lstore.page import Page
from lstore.storage.uid_gen import UIDGenerator
from lstore.storage.rid import RID

class PageTableEntry:
    def __init__(self, pages_id, tcols) -> None:
        self.data = [Page(pages_id) for _ in range(tcols)]

        self.id = pages_id
        self.bytes = 0

    def get_loc(self):
        return self.id, self.bytes

    def __getitem__(self, index):
        return self.data[index]
    
    def __setitem__(self, index, value):
        self.data[index] = value

    def __len__(self):
        return len(self.data)

    def __getattr__(self, attr):
        return getattr(self.data, attr)
    
    def has_capacity(self):
        return self.bytes + config.RECORD_SIZE <= config.PAGE_SIZE
    
    def write_vals(self, cols):
        for col, page in enumerate(self.data):
            page.write(cols[col])
            page.is_dirty = True

        self.bytes += config.RECORD_SIZE


class PageTable:
    pages_id_gen = UIDGenerator("pages_id", config.UID_DIR, RID.pages_id_bits, 10_000)

    def __init__(self, tcols) -> None:
        self.ptable = OrderedDict()
        
        self.tcols = tcols
        self.size = 0

    def __getattr__(self, attr):
        return getattr(self.ptable, attr)

    def create_pages(self):
        pages_id = PageTable.pages_id_gen.next_uid()

        pages = PageTableEntry(pages_id, self.tcols)

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

from collections import OrderedDict

from lstore import config

from lstore.page import Page
from lstore.storage.uid_gen import UIDGenerator
from lstore.storage.rid import RID

class PageTableEntry:
    record_size = config.RECORD_SIZE
    page_size = config.PAGE_SIZE

    def __init__(self, pages_id, total_cols, offset=None) -> None:
        self.data = [None for _ in range(total_cols)]

        self.pages_id = pages_id
        self.total_cols = total_cols

        self.page_count = 0
        
        self.offset = PageTableEntry.record_size if offset is None else offset

    def create_new_pages(self, pages_id):
        self.data = [Page(pages_id) for _ in range(self.total_cols)]
        self.page_count = self.total_cols

    def get_loc(self):
        return self.pages_id, self.offset

    def __getitem__(self, index):
        return self.data[index]
    
    def __setitem__(self, index, value):
        self.data[index] = value

    def __delitem__(self, index):
        del self.data[index]

    def __len__(self):
        return len(self.data)
    
    def has_capacity(self):
        return self.offset + PageTableEntry.record_size <= PageTableEntry.page_size
    
    def write_vals(self, cols):
        for col, page in enumerate(self.data):
            page.write(cols[col])
            page.is_dirty = True

        self.offset += PageTableEntry.record_size

    def pop_page(self, col):
        page = self[col]

        self.data[col] = None

        # Update size and delete entry if empty
        self.page_count -= 1
        if self.page_count <= 0:
            del self[col]

        return page

class PageTable:
    base_id_gen = None
    tail_id_gen = None

    @classmethod
    def initialize_uid_gen(cls, db_path):
        cls.base_id_gen = UIDGenerator("base_pages_id", db_path, 
            RID.pages_id_bits, even_only=True)
    
        cls.tail_id_gen = UIDGenerator("tail_pages_id", db_path,
            RID.pages_id_bits, even_only=True)

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
        # Assign even or odd pages_id based on base/tail record
        if is_base:
            pages_id = PageTable.base_id_gen.next_uid()      # Even
        else:
            pages_id = PageTable.tail_id_gen.next_uid() - 1  # Odd
        
        pages = PageTableEntry(pages_id, self.tcols)
        pages.create_new_pages(pages_id)

        self.ptable[pages_id] = pages

        self.size += self.tcols

        return pages, pages_id
    
    def init_pages(self, pages_id, offset):
        # Create page entry with no pages (filled with None)
        page_entry = PageTableEntry(pages_id, self.tcols, offset=offset)

        self.ptable[pages_id] = page_entry

        return self.ptable[pages_id]

    def get_pages(self, pages_id):
        return self.ptable.get(pages_id, None)

    def get_page(self, pages_id, col):
        page_entry = self.ptable.get(pages_id)
        if page_entry:
            return page_entry[col]
            
        return None

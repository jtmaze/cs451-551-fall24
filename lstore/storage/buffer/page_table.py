"""
Holds pages in memory.

The PageTable is a wrapper around a dictionary of PageTableEntries, which are
themselves wrappers around lists of pages per column.
"""

import threading
import threading

from lstore import config

from lstore.page import Page
from lstore.storage.uid_gen import UIDGenerator
from lstore.storage.rid import RID

class PageTableEntry:
    # Cache config params
    record_size = config.RECORD_SIZE
    page_size = config.PAGE_SIZE

    def __init__(self, pages_id: int, total_cols: int) -> None:
        self.pages = [None for _ in range(total_cols)]

        self.pages_id = pages_id
        self.total_cols = total_cols

        self.page_count = 0
        
        # Offset in bytes, ie how many bytes are occupied by each page
        self.offset = Page.header_size

    def __getitem__(self, index: int) -> Page:
        return self.pages[index]
    
    def __setitem__(self, index: int, value: Page | None):
        self.pages[index] = value

    def __len__(self):
        return len(self.pages)
    
    def create_new_pages(self, pages_id: int):
        self.pages = [Page(pages_id) for _ in range(self.total_cols)]
        self.page_count = self.total_cols

    def add_page(self, page: Page, col: int):
        self[col] = page
        self.page_count += 1

    def get_loc(self) -> tuple[int, int]:
        return self.pages_id, self.offset
    
    def has_capacity(self) -> bool:
        return self.offset + PageTableEntry.record_size <= PageTableEntry.page_size
    
    def write_vals(self, columns):
        for col, page in enumerate(self.pages):
            page.write(columns[col])
            page.is_dirty = True

        self.offset += PageTableEntry.record_size

    def delete_page(self, col: int) -> bool:
        self.pages[col] = None
        self.page_count -= 1

        return self.page_count <= 0

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
        self.ptable = dict()
        
        self.tcols = tcols
        self.size = 0

        self.lock = threading.Lock()

    def __iter__(self):
        return iter(self.ptable)
    
    def __getitem__(self, key):
        return self.ptable[key]
    
    def create_pages(self, is_base) -> tuple[PageTableEntry, int]:
        # Assign even or odd pages_id based on base/tail record
        if is_base:
            pages_id = PageTable.base_id_gen.next_uid()      # Even
        else:
            pages_id = PageTable.tail_id_gen.next_uid() - 1  # Odd
        
        # Init table entry and populate with pages
        pages = PageTableEntry(pages_id, self.tcols)
        pages.create_new_pages(pages_id)

        self.ptable[pages_id] = pages

        self.size += self.tcols

        return pages, pages_id
    
    def init_pages(self, pages_id) -> PageTableEntry:
        # Create page entry with no pages (filled with None)
        page_entry = PageTableEntry(pages_id, self.tcols)

        self.ptable[pages_id] = page_entry

        return self.ptable[pages_id]
    
    def get_entry(self, pages_id) -> PageTableEntry:
        return self.ptable.get(pages_id, None)
    
    def remove_page(self, pages_id, col) -> bool:
        if pages_id in self.ptable:
            pages = self.get_entry(pages_id)
            is_entry_empty = pages.delete_page(col)

            if is_entry_empty:
                self.ptable.pop(pages_id, None)
                return True

        self.size -= 1
        return False

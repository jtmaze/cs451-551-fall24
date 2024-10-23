import time
from collections import OrderedDict, namedtuple

from table import MetaColumn
from storage.rid import RID

import config

from page import Page

class Bufferpool:
    """
    A simple bufferpool that uses a hash table to store pages in memory,
    using RIDs (Record IDs) as keys.
    """
    RecordIndex = namedtuple("RecordIndex", ["page_id", "offset"])

    def __init__(self, num_columns: int):
        self.num_columns: int = num_columns
        self.total_columns: int = MetaColumn.COLUMN_COUNT + num_columns

        self.curr_page_id: int = 0  # Total pages created in memory
        self.page_count: int = 0    # Current number of pages
        self.max_buffer_size: int = config.MAX_BUFFER_SIZE

        # Maps page id -> page for each column (including metadata)
        self.pages = [
            OrderedDict() for _ in range(self.total_columns)
        ]

    def write(self, rid: RID, columns) -> list[RecordIndex]:
        """
        Writes a new record with the given data columns.

        Returns a list of RecordIndex objects to be used as values in the
        page directory.
        """
        page_dir_entry = []

        page_dir_entry.append(self._write_val(MetaColumn.INDIRECTION, 0))
        page_dir_entry.append(self._write_val(MetaColumn.RID, rid))
        page_dir_entry.append(self._write_val(MetaColumn.TIMESTAMP, time.now()))
        page_dir_entry.append(self._write_val(MetaColumn.SCHEMA_ENCODING, 0))

        for i in range(MetaColumn.COLUMN_COUNT, self.total_columns):
            page_dir_entry.append(self._write_val(i, columns[i]))

        return page_dir_entry

    def add_page(self, rid, page):
        """OLD!!!!
        Adds a page to the bufferpool and associates it with the given RID.
        """
        # If page already in buffer, move to end of queue
        if rid in self.pages:
            self.pages.move_to_end(rid)

        self.pages[rid] = page

        # Remove oldest if buffer full (first in ordered dict)
        if self.max_size and len(self.pages) > self.max_size:
            self.pages.popitem(last=False)

    def remove_page(self, rid):
        """OLD!!!!!!
        Removes a page from the bufferpool.
        """
        if rid in self.pages:
            del self.pages[rid]

    # Helpers ------------------------

    def _write_val(self, col, val) -> RecordIndex:
        """
        Writes the given value to the last page in the given column.

        If full, allocates a new page and writes there.
        """
        try:
            # O(1) and reasonably fast
            page: Page = next(reversed(self.pages[col]))
        except StopIteration:
            page = None

        if page is None or not page.has_capacity():
            # Create new page and update buffer pool
            page = Page(self.curr_page_id)
            self.pages[col][page.id] = page

            self.curr_page_id += 1
            self.page_count += 1

            if self.max_buffer_size and self.page_count > self.max_buffer_size:
                # TODO: handle full buffer
                #self.page_count -= 1
                pass

        offset = page.write(val)

        return Bufferpool.RecordIndex(page.id, offset)

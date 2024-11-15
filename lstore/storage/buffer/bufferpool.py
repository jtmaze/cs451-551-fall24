"""
The bufferpool contains the actual pages in memory. The data itself is to be
accessed via composite RecordIndex objects that encode page ids and data
offsets.
"""

from typing import Literal

from collections import OrderedDict  # MRU cache

from lstore.storage.record import Record
from lstore.storage.meta_col import MetaCol
from lstore.storage.rid import RID

from lstore.storage.buffer.page_table import PageTable, PageTableEntry

from lstore import config

from lstore.page import Page
from lstore.storage.disk import Disk


class Bufferpool:
    """
    A simple bufferpool that uses a hash table to store pages in memory,
    using RIDs (Record IDs) as keys.

    :param table: Reference to parent table
    """

    def __init__(self, table):
        self.table = table

        self.tcols = self.table.num_total_cols

        self.max_buffer_size = config.MAX_BUFFER_PAGES

        # Global page table for LRU/MRU eviction
        self.page_table = PageTable(self.tcols)  # Maps page_id -> list of page objects per column
        
        # Pointers to pages in page_table (used as ordered sets)
        self.base_trackers = OrderedDict()
        self.tail_trackers = OrderedDict()

        # Maps page id to (tracker, col idx) for evictions
        self.reverse_tracker = dict()

        self._new_vals_buffer = [None for _ in range(self.tcols)]

    def write(self, columns: tuple[int]) -> RID:
        """
        Writes a new record with the given data columns.
        Marks the page as dirty if modified.

        Returns a list of RecordIndex objects to be used as values in the
        page directory.

        :param columns: Tuple of data values for each column

        :return: new RID
        """
        # Create base rid
        pages_b: PageTableEntry = self._get_pages(True)
        pages_id_b, offset_b = pages_b.get_loc()
        rid: RID = RID.from_params(pages_id_b, offset_b, is_base=1, tombstone=0)

        # Create 'tail' rid (copy of base)
        pages_t: PageTableEntry = self._get_pages(False)
        pages_id_t, offset_t = pages_t.get_loc()
        tail_rid: RID = RID.from_params(pages_id_t, offset_t, is_base=0, tombstone=0)

        # Cache buffer
        new_vals = self._new_vals_buffer

        # Write base record
        new_vals[MetaCol.INDIR] = int(tail_rid)
        new_vals[MetaCol.RID] = int(rid)
        new_vals[MetaCol.SCHEMA] = 0
        new_vals[len(MetaCol):self.tcols] = columns # All data columns
        pages_b.write_vals(new_vals)

        # Write first tail record (copy of base record)
        new_vals[MetaCol.INDIR] = 0
        new_vals[MetaCol.RID] = int(tail_rid)
        pages_t.write_vals(new_vals)

        # Return new base rid for index
        return rid

    def update(self, rid: RID, tombstone: Literal[0, 1], columns: tuple[int | None]):
        """
        'Updates' a record by creating a new tail record and marks page as dirty.

        The indirection pointer of the base record and previous newest tail
        record are changed accordingly. The schema encoding of the base
        record is also updated to reflect which columns have ever been
        updated.

        :param rid: Base record RID
        :param tombstone: Value of tombstone flag (0 if updating, 1 if deleting)
        :param columns: New data values. Vals are none if no update for that col
        """
        pages_id_b, offset_b = rid.get_loc()

        self._validate_not_deleted(rid, pages_id_b, offset_b)

        # Create new RID
        pages: PageTableEntry = self._get_pages(False)
        pages_id_t, offset_t = pages.get_loc()
        tail_rid: RID = RID.from_params(pages_id_t, offset_t, is_base=0, tombstone=tombstone)

        # Cache for performance
        _read_val_cached = self._read_val

        new_vals = self._new_vals_buffer

        # Indirection -----------------

        # Set new tail indir to prev tail rid and base indir to new rid
        indir_rid = RID(_read_val_cached(MetaCol.INDIR, pages_id_b, offset_b))
        new_vals[MetaCol.INDIR] = int(indir_rid)
        self._overwrite_val(MetaCol.INDIR, rid, tail_rid)

        # RID ----------- -------------

        new_vals[MetaCol.RID] = int(tail_rid)

        # Schema encoding & data ------

        schema_encoding = _read_val_cached(MetaCol.SCHEMA, pages_id_b, offset_b)

        # Get record indices for previous tail record if cumulative updates
        pages_id_i = indir_rid.pages_id
        offset_i = indir_rid.pages_offset

        # Go through columns while updating schema encoding and data
        metalen = len(MetaCol)
        for data_col, val in enumerate(columns):
            real_col = metalen + data_col

            if val is None:
                # Get previous value if cumulative
                val = _read_val_cached(real_col, pages_id_i, offset_i)
            else:
                # Update schema by setting appropriate bit to 1
                schema_encoding |= (1 << data_col)

            new_vals[real_col] = val

        # Write both base and new tail record schema encoding
        new_vals[MetaCol.SCHEMA] = schema_encoding
        self._overwrite_val(MetaCol.SCHEMA, rid, schema_encoding)

        pages.write_vals(new_vals)

    def read(
            self,
            rid: RID,
            proj_col_idx: list[Literal[0, 1]],
            rel_version: int
    ) -> Record:
        """
        Reads a record (projected columns only) given an RID and its associated
        RecordIndex found in the page directory.

        :param rid: Base record RID
        :param proj_col_idx: List of 0s or 1s indicating which columns to return
        :param rel_version: Relative version to return. 0 is latest, -<n> are prev

        :return: Record with retrieved data in record.columns and base rid
        """
        pages_id, offset = rid.get_loc()

        self._validate_not_deleted(rid, pages_id, offset)

        # Cache for performance
        _read_val_cached = self._read_val

        # If a column has tail records, get record indices for correct version
        schema_encoding = _read_val_cached(MetaCol.SCHEMA, pages_id, offset)
        if schema_encoding:
            pages_id, offset = self._get_versioned_indices(
                pages_id, offset, rel_version)

        # Read projected data
        meta_len = len(MetaCol)
        columns = [
            _read_val_cached(i, pages_id, offset) 
            for i in range(meta_len, self.tcols) 
            if proj_col_idx[i - meta_len]
        ]

        return Record(self.table.key, columns, rid)

    def pin_page(self, page_id):
        """Increment the pin count for a specific page to prevent eviction."""
        page = self.page_table.get(page_id)
        if page:
            page.pin_count += 1

    def unpin_page(self, page_id):
        """Decrement the pin count for a specific page, allowing for eviction."""
        page = self.page_table.get(page_id)
        if page:
            page.pin_count = max(0, page.pin_count - 1)

    def flush_to_disk(self):
        """Flushes all pages in bufferpool's page table to the disk."""
        for pages_id in self.page_table:
            for i in range(self.tcols):
                self._flush_page_to_disk(pages_id, i)

    # Helpers ------------------------

    def _flush_page_to_disk(self, pages_id, col):
        page: Page | None = self.page_table[pages_id].pop_page(col)

        if page is not None and page.is_dirty:
            self.table.disk.add_page(page, pages_id, col)

            if config.DEBUG_PRINT:
                print(f"Flushed page {pages_id} in column {col} to disk.")

    def _evict_page(self):
        raise NotImplementedError()
        # Search for most recently used UNPINNED page to evict from cache
        for pages_id in reversed(self.page_table):
            page = self.page_table[pages_id]

            if page.pin_count <= 0:
                del self.page_table[pages_id]

                # Delete page from tracker
                if pages_id in self.reverse_tracker:
                    tracker, col = self.reverse_tracker.pop(pages_id)
                    del tracker[col][pages_id]

                # Cleanup
                self.page_count -= 1
                if page.is_dirty:
                    self._flush_page_to_disk(page)

                return

        raise RuntimeWarning("Tried to evict a page, but no unpinned pages available!")

    def _get_pages(self, is_base):
        page_tracker = self.base_trackers if is_base else self.tail_trackers

        if page_tracker:
            pages_id = next(reversed(page_tracker))
            pages: PageTableEntry = self.page_table.get_pages(pages_id)
        else:
            pages = None

        # TODO: Fetch from disk?

        # Create new pages if necessary
        if pages is None or not pages.has_capacity():
            pages, pages_id = self.page_table.create_pages(is_base)

            if self.max_buffer_size and self.page_table.size > self.max_buffer_size:
                self._evict_page()

            page_tracker[pages_id] = None  # Value doesn't matter, used as ordered set

            # self.reverse_tracker[page.id] = (page_tracker, col)

        # Move to end for cache eviction
        self.page_table.move_to_end(pages_id, last=True)

        return pages

    def _overwrite_val(self, col:int, rid: RID, val: int):
        """
        Overwrites a value in a page, marking it as dirty.
        """
        pages_id, offset = rid.get_loc()

        page = self.page_table.get_page(pages_id, col)
        if page is None:
            page = self._fetch_page_from_disk(pages_id, col)

        page.update(val, offset)
        page.is_dirty = True

        self.page_table.move_to_end(pages_id, last=True)

    def _read_val(self, col: int, pages_id: int, offset: int):
        """
        Reads a value from a page given a column (including metadata cols)
        and a page id/offset.
        """
        page = self.page_table.get_page(pages_id, col)
        if page is None:
            page = self._fetch_page_from_disk(pages_id, col)

        self.page_table.move_to_end(pages_id, last=True)

        return page.read(offset)

    def _get_versioned_indices(self, pages_id, offset, rel_version):
        """
        Given base record indices, gets record indices for a given relative
        version. Will always go to most recent tail record (version 0) at least.
        """
        # Will do it at least once since version 0 is newest tail record
        while rel_version <= 0:
            # Get previous tail record (or base record). base.indir == base.rid!
            indir = RID(self._read_val(MetaCol.INDIR, pages_id, offset))

            if indir <= 0 or indir.is_base:
                break

            pages_id, offset = indir.get_loc()

            rel_version += 1

        return pages_id, offset

    def _validate_not_deleted(self, rid, pages_id, offset):
        if RID(self._read_val(MetaCol.INDIR, pages_id, offset)).tombstone:
            raise KeyError(f"Record {rid} was deleted")

    def _fetch_page_from_disk(self, pages_id, col):
        """
        Fetch a page from the disk.
        """
        disk: Disk = self.table.disk

        # Create page from disk
        page = disk.get_page(pages_id, col)
        
        # Populate page table entry
        pages: PageTableEntry = self.page_table.get_pages(pages_id)
        if pages is None:
            pages = self.page_table.init_pages(pages_id)
        pages[col] = page

        self.page_table.move_to_end(pages_id, last=True)

        return page

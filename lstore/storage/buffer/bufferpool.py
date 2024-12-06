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

        # Cache config
        self.use_lru = config.USE_LRU_NOT_MRU
        self.max_buffer_size = config.MAX_BUFFER_PAGES
        if self.max_buffer_size is not None:
            # Ensure the buffer size can at least contain sets of read/write base/tail pages (4)
            self.max_buffer_size = max(self.tcols * 4, self.max_buffer_size)

        # Global page table for LRU/MRU eviction
        # Maps page_id -> list of page objects per column
        self.page_table = PageTable(self.tcols)

        # Pointers to pages in page_table (only as ordered sets)
        # Allows tracking base/tails in memory and and getting latest for writing
        self.base_trackers = OrderedDict()
        self.tail_trackers = OrderedDict()

        # Saves page id and col as tuple in key for eviction
        self.evict_queue = OrderedDict()

        self._new_vals_buffer = [None for _ in range(self.tcols)]

    def write(self, columns: tuple[int]) -> RID:
        """
        Writes a new record w/ the given data columns.
        Marks the page as dirty if modified.

        Returns a list of RecordIndex objects to be used as values in the
        page directory.

        :param columns: Tuple of data values for each column

        :return: new RID
        """
        with self.page_table.lock:
            pages_b = self._get_latest_page_entry(True)
            pages_t = self._get_latest_page_entry(False)

            # Create base rid
            pages_id_b, offset_b = pages_b.get_loc()
            rid: RID = RID.from_params(
                pages_id_b, offset_b, is_base=1, tombstone=0)

            # Create 'tail' rid (copy of base)
            pages_id_t, offset_t = pages_t.get_loc()
            tail_rid: RID = RID.from_params(
                pages_id_t, offset_t, is_base=0, tombstone=0)

            # Cache buffer
            new_vals = self._new_vals_buffer

            # Write base record
            new_vals[MetaCol.INDIR] = int(tail_rid)
            new_vals[MetaCol.RID] = int(rid)
            new_vals[MetaCol.SCHEMA] = 0
            new_vals[len(MetaCol):self.tcols] = columns  # All data columns
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
        with self.page_table.lock:
            pages_id_b, offset_b = rid.get_loc()
            self._validate_not_deleted(rid, pages_id_b, offset_b)

            pages_b = self.page_table.get_entry(pages_id_b)
            pages_t = self._get_latest_page_entry(False)

            # Create new RID
            pages_id_t, offset_t = pages_t.get_loc()
            tail_rid = RID.from_params(
                pages_id_t, offset_t, is_base=0, tombstone=tombstone)

            # Cache for performance
            _read_val_cached = self._read_val
            new_vals = self._new_vals_buffer

            # Indirection -----------------

            # Set new tail indir to prev tail rid and base indir to new rid
            indir_rid = RID(_read_val_cached(
                MetaCol.INDIR, pages_id_b, offset_b))
            new_vals[MetaCol.INDIR] = int(indir_rid)
            self._overwrite_val(MetaCol.INDIR, rid, tail_rid, pages_b)

            # RID ----------- -------------

            new_vals[MetaCol.RID] = int(tail_rid)

            # Schema encoding & data ------

            # Get record indices for previous tail record
            pages_id_i, offset_i = indir_rid.get_loc()

            # Get latest schema encoding (go to latest tail if recently merged)
            schema_encoding = _read_val_cached(
                MetaCol.SCHEMA, pages_id_b, offset_b)
            # If latest tail record previously merged into base record
            if schema_encoding == -1:
                schema_encoding = _read_val_cached(
                    MetaCol.SCHEMA, pages_id_i, offset_i)

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
            self._overwrite_val(MetaCol.SCHEMA, rid, schema_encoding, pages_b)

            pages_t.write_vals(new_vals)

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

        :return: Record w/ retrieved data in record.columns and base rid
        """
        pages_id, offset = rid.get_loc()

        self._validate_not_deleted(rid, pages_id, offset)

        # Cache for performance
        _read_val_cached = self._read_val

        # If a column has tail records, get record indices for correct version
        schema_encoding = _read_val_cached(MetaCol.SCHEMA, pages_id, offset)

        # If schema encoding is -1 (ie latest merged into base), else if updated...
        if schema_encoding == -1 and rel_version == 0:
            pages_id, offset = rid.get_loc()
        elif schema_encoding:
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

    def restore(self, rid: RID):
        """
        Undoes delete/update by moving indirection back one element
        :param rid: RID of the record to restore
        """
        try:
            pages_id, offset = rid.get_loc()

            tail_rid = RID(self._read_val(MetaCol.INDIR, pages_id, offset))
            pages_id_prev, offset_prev = tail_rid.get_loc()

            prev_rid = self._read_val(
                MetaCol.INDIR, pages_id_prev, offset_prev)

            self._overwrite_val(MetaCol.INDIR, rid, prev_rid)

            if config.DEBUG_PRINT:
                print(f"Record {rid} restored successfully.")
        except Exception as e:
            print(f"Error restoring record {rid}: {e}")

    def flush_to_disk(self):
        """Flushes all pages in bufferpool's page table to the disk."""
        for pages_id in self.page_table:
            pages = self.page_table.get_entry(pages_id)

            for col in range(self.tcols):
                self._flush_page_to_disk(pages[col], pages_id, col)

    # Helpers ------------------------

    def _get_latest_page_entry(self, is_base) -> PageTableEntry:
        page_tracker = self.base_trackers if is_base else self.tail_trackers

        if page_tracker:
            pages_id = next(reversed(page_tracker))
            pages: PageTableEntry = self.page_table.get_entry(pages_id)
        else:
            pages = None

        if pages is None:
            pages, pages_id = self.page_table.create_pages(is_base)
            # Value doesn't matter, used as ordered set
            page_tracker[pages_id] = None
        elif not pages.has_capacity():
            pages, pages_id = self.page_table.create_pages(is_base)

            # Add full pages to the evict queue
            if self.max_buffer_size:
                for col in range(self.tcols):
                    self.evict_queue[(pages_id, col)] = None
                    self.evict_queue.move_to_end(
                        (pages_id, col), last=self.use_lru)
                    self._evict_pages()

            # Value doesn't matter, used as ordered set
            page_tracker[pages_id] = None

        return pages

    def _overwrite_val(self, col: int, rid, val: int, pages: PageTableEntry = None):
        pages_id, offset = rid.get_loc()

        # Get page entry if not given (create empty one if needed)
        if pages is None:
            pages = self.page_table.get_entry(pages_id)
            if pages is None:
                pages = self.page_table.init_pages(pages_id)

        # Get page (from disk if necessary)
        page = pages[col]
        if page is None:
            page = self._get_page_from_disk(pages, pages_id, col)

        pages.offset = page.offset

        self._update_evict_queue(pages_id, col)

        page.update(val, offset)
        page.is_dirty = True

    def _read_val(self, col: int, pages_id: int, offset: int):
        """
        Reads a value from a page given a column (including metadata cols)
        and a page id/offset.
        """
        # Get page entry (create empty one if needed)
        pages = self.page_table.get_entry(pages_id)
        if pages is None:
            pages = self.page_table.init_pages(pages_id)

        # Get page (from disk if necessary)
        page = pages[col]
        if page is None:
            page = self._get_page_from_disk(pages, pages_id, col)

        pages.offset = page.offset

        self._update_evict_queue(pages_id, col)

        return page.read(offset)

    def _get_page_from_disk(self, pages: PageTableEntry, pages_id: int, col: int):
        """
        Gets page from disk, adds it to page entry and adds to evict queue.
        """
        # Create page from disk and add to entry
        disk: Disk = self.table.disk
        page = disk.get_page(pages_id, col)

        pages.add_page(page, col)

        self._add_to_evict_queue(pages_id, col)

        return page

    def _add_to_evict_queue(self, pages_id, col):
        if self.max_buffer_size:
            self.evict_queue[(pages_id, col)] = None

    def _update_evict_queue(self, pages_id, col):
        if self.max_buffer_size:
            try:
                self.evict_queue.move_to_end(
                    (pages_id, col), last=self.use_lru)
                self._evict_pages()
            except KeyError:
                pass

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
            raise KeyError(f"Record {int(rid)} was deleted")

    def _evict_pages(self):
        if self.max_buffer_size:
            while len(self.evict_queue) > self.max_buffer_size:
                self._evict_latest_page()

    def _evict_latest_page(self):
        # Remove location of page to evict from queue
        pages_id, col = self.evict_queue.popitem(last=False)[0]

        pages = self.page_table.get_entry(pages_id)

        page = pages[col]

        is_entry_empty = self.page_table.remove_page(pages_id, col)

        # Also remove page from head/page trackers
        if is_entry_empty:
            tracker = self.tail_trackers if pages_id % 2 else self.base_trackers
            tracker.pop(pages_id, None)

        # Write to disk (will check if dirty)
        self._flush_page_to_disk(page, pages_id, col)

    def _flush_page_to_disk(self, page, pages_id, col):
        """Currently writes page to disk."""
        if page is not None and page.is_dirty:
            self.table.disk.add_page(page, pages_id, col)

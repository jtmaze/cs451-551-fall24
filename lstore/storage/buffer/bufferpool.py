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
from lstore.storage.record_index import RecordIndex

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

        self.curr_page_id = 0  # Total pages created in memory
        self.page_count = 0  # Current number of pages
        self.max_buffer_size = config.MAX_BUFFER_PAGES

        # Global page table for MRU eviction
        self.page_table = OrderedDict()  # Maps page_id -> page object

        # Pointers to pages in page_table (used as ordered sets)
        self.base_trackers = [OrderedDict() for _ in range(self.tcols)]
        self.tail_trackers = [OrderedDict() for _ in range(self.tcols)]

        # Maps page id to (tracker, col idx) for evictions
        self.reverse_tracker = dict()

        #page id stuff
        self.disk = Disk()

    def write(self, rid: RID, columns: tuple[int]) -> list[RecordIndex]:
        """
        Writes a new record with the given data columns.
        Marks the page as dirty if modified.

        Returns a list of RecordIndex objects to be used as values in the
        page directory.

        :param rid: New RID for base record to be stored in page dir as key
        :param columns: Tuple of data values for each column

        :return: List of composite RecordIndices to store as val in page dir
        """
        record_indices = [None for _ in range(self.tcols)]

        # Write metadata, saving record indices per column
        record_indices[MetaCol.INDIR] = self._write_val(MetaCol.INDIR, rid, self.base_trackers)
        record_indices[MetaCol.RID] = self._write_val(MetaCol.RID, rid, self.base_trackers)
        record_indices[MetaCol.SCHEMA] = self._write_val(MetaCol.SCHEMA, 0, self.base_trackers)

        # Write data, saving record indices per remaining columns
        for i in range(len(MetaCol), self.tcols):
            record_indices[i] = self._write_val(
                i, columns[i - len(MetaCol)], self.base_trackers)

        # Return indices to be stored as values in page directory
        return record_indices

    def update(self, rid: RID, tail_rid: RID, columns: tuple[int | None]):
        """
        'Updates' a record by creating a new tail record and marks page as dirty.

        The indirection pointer of the base record and previous newest tail
        record are changed accordingly. The schema encoding of the base
        record is also updated to reflect which columns have ever been
        updated.

        :param rid: Base record RID
        :param tail_rid: New tail record RID registered in page dir
        :param columns: New data values. Vals are none if no update for that col
        """
        base_indices = self._get_base_indices(rid)

        self._validate_not_deleted(rid, base_indices)

        tail_indices = [None for _ in range(self.tcols)]

        # Indirection -----------------

        # Set new tail indir to prev tail rid and base indir to new rid
        prev_rid = RID(self._read_meta(base_indices, MetaCol.INDIR))
        tail_indices[MetaCol.INDIR] = self._write_val(
            MetaCol.INDIR, prev_rid, self.tail_trackers)
        self._overwrite_val(rid, MetaCol.INDIR, tail_rid)

        # RID ----------- -------------

        tail_indices[MetaCol.RID] = self._write_val(MetaCol.RID, tail_rid, self.tail_trackers)

        # Schema encoding & data ------

        schema_encoding = self._read_meta(base_indices, MetaCol.SCHEMA)

        # Get record indices for previous tail record if cumulative updates
        prev_indices = self.table.buffer.page_dir[prev_rid]

        # Go through columns while updating schema encoding and data
        for data_col, val in enumerate(columns):
            real_col = len(MetaCol) + data_col

            if val is not None:
                # Update schema by setting appropriate bit to 1
                schema_encoding = schema_encoding | (1 << data_col)
            else:
                # Get previous value if cumulative
                val = self._read_val(real_col, prev_indices[real_col])

            tail_indices[real_col] = self._write_val(real_col, val, self.tail_trackers)

        # Write both base and new tail record schema encoding
        tail_indices[MetaCol.SCHEMA] = self._write_val(
            MetaCol.SCHEMA, schema_encoding, self.tail_trackers)
        self._overwrite_val(rid, MetaCol.SCHEMA, schema_encoding)

        return tail_indices

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
        record_indices = self._get_base_indices(rid)

        self._validate_not_deleted(rid, record_indices)

        # If a column has tail records, get record indices for correct version
        schema_encoding = self._read_meta(record_indices, MetaCol.SCHEMA)
        if schema_encoding:
            record_indices = self._get_versioned_indices(
                record_indices, rel_version)

        # Get enumerated projection of (page ID, offset) pairs for cols from page dir
        #   ex. record index = [(p_0, o_0), (p_1, o_1), (p_2, o_2)]
        #       proj_col_idx = [0, 1, 1]
        #       --------------------------
        #       record_indices = [(1, (p_1, o_1)), (2, (p_2, o_2))]
        data_indices = record_indices[len(MetaCol):]
        data_indices: list[tuple[int, RecordIndex]] = [
            (col_idx, r_idx) for col_idx, r_idx in enumerate(data_indices) if proj_col_idx[col_idx]
        ]

        columns = []
        for col_idx, r_idx in data_indices:
            columns.append(self._read_val(col_idx + len(MetaCol), r_idx))

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

    # Helpers ------------------------

    def _create_new_page(self):
        page = Page(self.curr_page_id)

        self.curr_page_id += 1
        self.page_count += 1

        return page

    def _flush_page_to_disk(self, page):
        pass

    def _evict_page(self):
        # Search for most recently used UNPINNED page to evict from cache
        for page_id in reversed(self.page_table):
            page = self.page_table[page_id]

            if page.pin_count <= 0:
                del self.page_table[page_id]

                # Delete page from tracker
                if page_id in self.reverse_tracker:
                    tracker, col = self.reverse_tracker.pop(page_id)
                    del tracker[col][page_id]

                # Cleanup
                self.page_count -= 1
                if page.is_dirty:
                    self._flush_page_to_disk(page)

                return

        raise RuntimeWarning("Tried to evict a page, but no unpinned pages available!")

    def _write_val(self, col: int, val: int, page_trackers: list[OrderedDict]) -> RecordIndex:
        """
        Writes the given value to the last page in the given column, marking it as dirty.

        If full, allocates a new page and writes there.
        """
        ptrack = page_trackers[col]

        if ptrack:
            pid = next(reversed(ptrack))
            page = self.page_table[pid]
        else:
            page = None

        if page is None or not page.has_capacity():
            page = self._create_new_page()

            ptrack[page.id] = None  # Value doesn't matter, used as ordered set
            self.reverse_tracker[page.id] = (page_trackers, col)

            # Update page table (and address MRU eviction)
            self.page_table[page.id] = page
            self.page_table.move_to_end(page.id, last=True)
            if self.max_buffer_size and self.page_count > self.max_buffer_size:
                self._evict_page()
        else:
            self.page_table.move_to_end(page.id, last=True)

        page.is_dirty = True
        offset = page.write(val)

        return RecordIndex(page.id, offset)

    def _overwrite_val(self, rid: RID, col: int, val: int):
        """
        Overwrites a value in a page, marking it as dirty.
        """
        r_idx: RecordIndex = self.table.buffer.page_dir[rid][col]
        page = self.page_table[r_idx.page_id]
        page.update(val, r_idx.offset)
        page.is_dirty = True

        self.page_table.move_to_end(r_idx.page_id, last=True)

    def _read_val(self, col: int, r_idx: RecordIndex):
        """
        Reads a value from a page given a column (including metadata cols)
        and a RecordIndex.
        """
        page = self.page_table.get(r_idx.page_id)

        self.page_table.move_to_end(r_idx.page_id, last=True)

        return page.read(r_idx.offset)

    def _read_meta(self, record_indices, metacol):
        return self._read_val(metacol, record_indices[metacol])

    def _get_base_indices(self, rid: RID):
        return self.table.buffer.page_dir[rid]

    def _get_versioned_indices(self, record_indices, rel_version):
        """
        Given base record indices, gets record indices for a given relative
        version. Will always go to most recent tail record (version 0) at least.
        """
        # Will do it at least once since version 0 is newest tail record
        while rel_version <= 0:
            # Get previous tail record (or base record). base.indir == base.rid!
            rid = RID(self._read_meta(record_indices, MetaCol.INDIR))

            record_indices = self.table.buffer.page_dir[rid]

            if rid.is_base:
                break

            rel_version += 1

        return record_indices

    def _validate_not_deleted(self, rid, record_indices):
        if RID(self._read_meta(record_indices, MetaCol.INDIR)).tombstone:
            raise KeyError(f"Record {rid} was deleted")

    def fetch_page(self, page_id):
        """
        Fetch a page by page_id. If it’s not in memory, load it from disk.
        """
        page = self.page_table.get(page_id)
        if page is None:
            page_data = self.disk.get_page(page_id)
            page = Page(page_id=page_id)
            page.data = bytearray(page_data)
            self.page_table[page_id] = page
            self.page_table.move_to_end(page_id, last=True)
        return page

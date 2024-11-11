"""
The bufferpool contains the actual pages in memory. The data itself is to be
accessed via composite RecordIndex objects that encode page ids and data
offsets.
"""

from typing import Literal

import time

from collections import OrderedDict # MRU cache

from lstore.storage.record import Record
from lstore.storage.meta_col import MetaCol
from lstore.storage.rid import RID
from lstore.storage.record_index import RecordIndex

from lstore import config

from lstore.page import Page


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
        self.page_count = 0    # Current number of pages
        self.max_buffer_size = config.MAX_BUFFER_PAGES

        # Maps page id -> page for each column (including metadata)
        self.pages = [
            dict() for _ in range(self.tcols)
        ]

        # self.base = [OrderedDict() for _ in range(self.tcols)]
        # self.tail = [OrderedDict() for _ in range(self.tcols)]

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
        record_indices[MetaCol.INDIR] = self._write_val(MetaCol.INDIR, rid)
        record_indices[MetaCol.RID] = self._write_val(MetaCol.RID, rid)
        record_indices[MetaCol.SCHEMA] = self._write_val(MetaCol.SCHEMA, 0)

        # Write data, saving record indices per remaining columns
        for i in range(len(MetaCol), self.tcols):
            record_indices[i] = self._write_val(
                i, columns[i - len(MetaCol)])

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
        self._validate_cumulative_update()

        base_indices = self._get_base_indices(rid)

        self._validate_not_deleted(rid, base_indices)

        tail_indices = [None for _ in range(self.tcols)]

        # Indirection -----------------

        # Set new tail indir to prev tail rid and base indir to new rid
        prev_rid = RID(self._read_meta(base_indices, MetaCol.INDIR))
        tail_indices[MetaCol.INDIR] = self._write_val(
            MetaCol.INDIR, prev_rid)
        self._overwrite_val(rid, MetaCol.INDIR, tail_rid)

        # RID ----------- -------------

        tail_indices[MetaCol.RID] = self._write_val(MetaCol.RID, tail_rid)

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

            tail_indices[real_col] = self._write_val(real_col, val)

        # Write both base and new tail record schema encoding
        tail_indices[MetaCol.SCHEMA] = self._write_val(
            MetaCol.SCHEMA, schema_encoding)
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
        for column in self.pages:
            if page_id in column:
                page = column[page_id]
                page.pin_count += 1

    def unpin_page(self, page_id):
        """Decrement the pin count for a specific page, allowing for eviction."""
        for column in self.pages:
            if page_id in column:
                page = column[page_id]
                page.pin_count = max(0, page.pin_count - 1)

    # Helpers ------------------------

    def _write_val(self, col: int, val: int) -> RecordIndex:
        """
        Writes the given value to the last page in the given column, marking it as dirty.

        If full, allocates a new page and writes there.
        """
        # O(1) and efficient
        page: Page = next(reversed(self.pages[col].values()), None)

        if page is None or not page.has_capacity():
            # Create new page and update buffer pool
            page = self._create_new_page()
            self.pages[col][page.id] = page

            # TODO: handle full buffer
            if self.max_buffer_size and self.page_count > self.max_buffer_size:
                # self.page_count -= 1
                raise NotImplementedError(
                    "Max buffer size specified, but not implemented yet")

        page.is_dirty = True
        offset = page.write(val)

        return RecordIndex(page.id, offset)
    
    def _create_new_page(self):
        page = Page(self.curr_page_id)

        self.curr_page_id += 1
        self.page_count += 1

        return page

    def _overwrite_val(self, rid: RID, col: int, val: int):
        """
        Overwrites a value in a page, marking it as dirty.
        """
        r_idx: RecordIndex = self.table.buffer.page_dir[rid][col]
        page = self.pages[col][r_idx.page_id]
        page.update(val, r_idx.offset)
        page.is_dirty = True

    def _read_val(self, col: int, r_idx: RecordIndex):
        """
        Reads a value from a page given a column (including metadata cols)
        and a RecordIndex.
        """
        return self.pages[col][r_idx.page_id].read(r_idx.offset)

    def _read_meta(self, record_indices, metacol):
        return self._read_val(metacol, record_indices[metacol])

    def _get_base_indices(self, rid: RID):
        return self.table.buffer.page_dir[rid]

    def _get_versioned_indices(self, record_indices, rel_version):
        """
        Given base record indices, gets record indices for a given relative
        version. Will always go to most recent tail record (version 0) at least.
        """
        self._validate_cumulative_update()

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

    @staticmethod
    def _validate_cumulative_update():
        if not config.CUMULATIVE_UPDATE:
            raise NotImplementedError(
                "Noncumulative update not finished, set config.CUMULATIVE_UPDATE to True")

    @staticmethod
    def _get_timestamp():
        return int(time.time() * 1000)  # millisecond

import time
from collections import OrderedDict

from table import Record, MetaCol
from storage.rid import RID
from storage.buffer import RecordIndex

import config

from page import Page


class Bufferpool:
    """
    A simple bufferpool that uses a hash table to store pages in memory,
    using RIDs (Record IDs) as keys.
    """

    def __init__(self, table):
        self.table = table

        self.total_columns: int = MetaCol.COL_COUNT + self.table.num_columns

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
        record_indices = []

        # Write metadata, saving record indices per column
        record_indices.append(self._write_val(MetaCol.INDIR, 0))
        record_indices.append(self._write_val(MetaCol.RID, rid))
        record_indices.append(self._write_val(MetaCol.TIMESTAMP, time.now()))
        record_indices.append(self._write_val(MetaCol.SCHEMA, 0))

        # Write data, saving record indices per remaining columns
        for i in range(MetaCol.COL_COUNT, self.total_columns):
            record_indices.append(self._write_val(i, columns[i]))

        # Return indices to be stored as values in page directory
        return record_indices

    def read(self, rid: RID, proj_col_idx, record_indices) -> Record:
        schema_encoding = self._read_page(
            MetaCol.SCHEMA, record_indices[MetaCol.SCHEMA])

        # If a column has tail records, switch to cumulative updated tail record
        if schema_encoding:
            tail_rid = self._read_page(
                MetaCol.INDIR, record_indices[MetaCol.INDIR])
            record_indices = self.page_dir[tail_rid]

        ##########

        data_indices = record_indices[MetaCol.COL_COUNT:]

        # Get enumerated projection of (page ID, offset) pairs for cols from page_dir
        #   ex. record index = [(p_0, o_0), (p_1, o_1), (p_2, o_2)]
        #       proj_col_idx = [0, 1, 1]
        #       --------------------------
        #       record_indices = [(1, (p_1, o_1)), (2, (p_2, o_2))]
        data_indices: list[tuple[int, RecordIndex]] = [
            (col_idx, r_idx) for col_idx, r_idx in enumerate(data_indices) if proj_col_idx[col_idx]
        ]

        ############

        columns = []
        for col_idx, r_idx in data_indices:
            columns.append(self._read_page(col_idx + MetaCol.COL_COUNT, r_idx))

        return Record(self.table.key, columns, rid)

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
                # self.page_count -= 1
                raise NotImplementedError(
                    "Max buffer size specified, but not implemented yet")

        offset = page.write(val)

        return RecordIndex(page.id, offset)

    def _read_page(self, col: int, r_idx: RecordIndex):
        """
        Reads a value from a page given a column (including metadata cols)
        and a RecordIndex.
        """
        return self.pages[col][r_idx.page_id].read(r_idx.offset)

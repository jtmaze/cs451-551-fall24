"""
Buffer for data yet to be lazily written to pages.
"""

import array

from lstore import config
from lstore.storage.meta_col import MetaCol
from lstore.storage.record import Record

class WriteQueue:
    def __init__(self, table) -> None:
        self.table = table

        self.max_records = config.WRITE_QUEUE_ROWS
        self.max_versions = config.WRITE_QUEUE_COLS

        self.dcols = self.table.num_columns

        self.size = 0

        # 3D data buffer: (data column, data record, version)
        #   2d array per record column
        #   Each row is a stack of versions of a data record (fixed size array)
        #   Each value per row is a new version
        self.arr = [
            [
                # Create a 2D array of 0s (integers) per record
                array.array("i", [0] * self.max_versions) for _ in range(self.max_records)
            ] for _ in range(self.dcols)
        ]

        # Per-row index pointers indicating top of stack
        self.stack_pointers = array.array("i", [0] * self.max_records)

        # Maps rid to arrays' row indices
        self.rid_map = dict()

    def write(self, rid, columns):
        """
        Writes or updates data given an RID. May also trigger queue flush.
        """
        # Update if rid in write queue, otherwise insert for new row
        if rid in self.rid_map:
            row_idx = self.rid_map[rid]
        else:
            row_idx = self.size
            self.rid_map[rid] = row_idx
            self.size += 1

        # Append value and increment
        col_idx = self.stack_pointers[row_idx]
        for i in range(self.dcols):
            self.arr[i][row_idx][col_idx] = columns[i]
        self.stack_pointers[row_idx] += 1

    def read(self, rid, rel_version, proj_col_idx) -> Record:
        """
        :raises KeyError: If RID not in write queue
        """
        row_idx = self.rid_map[rid]

        columns = []

        for i in range(self.dcols):
            if proj_col_idx[i]:
                col_idx = max(self.stack_pointers[i][row_idx] + rel_version, 0)

                columns.append(self.arr[i][row_idx][col_idx])

        return Record(self.table.key, columns, rid)


    # Helper -------

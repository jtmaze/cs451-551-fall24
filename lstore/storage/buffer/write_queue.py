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
        self.mcols = len(MetaCol)

        self.size = 0

        # 3D data buffer: (column value, data record, version)
        #   2d array per record column
        #   Each row is a stack of versions of a data record (fixed size array)
        #   Each value per row is a new version
        self.data = self._create_3d_buffer(self.dcols)
        self.meta = self._create_3d_buffer(self.mcols)

        # Per-row index pointers indicating top of stack
        self.stack_pointers = array.array("i", [0] * self.max_records)

        # Maps rid to arrays' row indices
        self.rid_map = dict()

    def write(self, rid, tail_rid, columns) -> bool:
        """
        Writes or updates data given an RID. Returns bool indicating if flush
        required.

        :returns: Indicates if flush needed
        """
        # Is it an update for rids already in write queue?
        if rid in self.rid_map:
            row_idx = self.rid_map[rid]
        else:
            row_idx = self.size
            self.rid_map[rid] = row_idx
            self.size += 1

        # Update data
        col_idx = self.stack_pointers[row_idx]
        for i in range(self.dcols):
            self.data[i][row_idx][col_idx] = columns[i]

        # Try to get previous indir/schema if in write queue
        # Will have to update leftmost when actually writing to page
        try:
            indir = self.meta[MetaCol.RID][row_idx][col_idx - 1]
            schema_encoding = self.meta[MetaCol.SCHEMA][row_idx][col_idx - 1]
        except KeyError:
            indir = 0
            schema_encoding = 0

        # Update metadata
        self.meta[MetaCol.INDIR][row_idx][col_idx] = indir
        self.meta[MetaCol.RID][row_idx][col_idx] = tail_rid
        for i, val in enumerate(columns):
            if val is not None:
                schema_encoding |= (1 << i)
        self.meta[MetaCol.SCHEMA][row_idx][col_idx] = schema_encoding

        # Update stack pointer
        self.stack_pointers[row_idx] += 1
        if self.size >= self.max_records or self.stack_pointers[row_idx] >= self.max_versions:
            return True
        
        return False
    
    def read(self, rid, rel_version, proj_col_idx) -> Record:
        """
        :raises KeyError: If RID not in write queue
        """
        row_idx = self.rid_map[rid]

        columns = []

        for i in range(self.dcols):
            if proj_col_idx[i]:
                col_idx = max(self.stack_pointers[i][row_idx] + rel_version, 0)

                columns.append(self.data[i][row_idx][col_idx])

        return Record(self.table.key, columns, rid)
    
    def flush_generator(self):
        for _ in range(max(self.stack_pointers)):
            rids = []
            data_batch = []
            meta_batch = []

            for rid, row_idx in list(self.rid_map.items()):
                col_idx = self.stack_pointers[row_idx] - 1  # Top of the stack

                if col_idx < 0:
                    continue  # No data to flush for this RID

                # Append the RID
                rids.append(rid)

                # Retrieve the most recent data and metadata for the RID
                data_batch.append([self.data[i][row_idx][col_idx] for i in range(self.dcols)])
                meta_batch.append([self.meta[i][row_idx][col_idx] for i in range(self.mcols)])

                # Decrement the stack pointer for this row
                self.stack_pointers[row_idx] -= 1

                # If there are no more versions left, remove the RID from rid_map and adjust size
                if self.stack_pointers[row_idx] == 0:
                    del self.rid_map[rid]
                    self.size -= 1

            # Yield the batch of RIDs, data, and metadata for this version level
            yield (rids, data_batch, meta_batch)
    

    # Helper -------

    def _create_3d_buffer(self, num_cols):
        return [
            [
                array.array("i", [0] * self.max_versions) for _ in range(self.max_records)
            ] for _ in range(num_cols)
        ]

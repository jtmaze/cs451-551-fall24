from typing import Literal

# TODO: For LRU cache, but large memory footprint
from collections import OrderedDict

from lstore.storage.bufferpool import Bufferpool
from lstore.storage.record_index import RecordIndex
from lstore.storage.disk import Disk
from lstore.storage.rid import RID, rid_generator

from lstore.storage.record import Record

class Buffer:
    """
    """

    def __init__(self, table) -> None:
        self.table = table

        self._rid_gen = rid_generator()

        # In memory -------------------

        # Maps RIDs to (logical page ID, offset) pairs per column. Ordered by staleness
        self.page_dir: OrderedDict[RID, list[RecordIndex]] = OrderedDict()

        # Maps (logical page id->page) for each column (including metadata)
        self.bufferpool = Bufferpool(self.table)

        # Disk ------------------------

        self.disk = Disk()  # TODO: Support persistent memory

    def insert_record(self, record: Record) -> RID:
        rid: RID = next(self._rid_gen)

        # Write and insert record indices per each column into page dir
        self.page_dir[rid] = self.bufferpool.write(rid, record.columns)

        return rid

    def update_record(self, rid, columns):
        tail_rid: RID = next(self._rid_gen)

        self.page_dir[tail_rid] = self.bufferpool.update(rid, tail_rid, columns)

    def get_record(
        self,
        rid: RID,
        proj_col_idx: list[Literal[0, 1]],
        rel_version: int
    ) -> Record | None:
        record_indices = self.page_dir[rid]

        return self.bufferpool.read(rid, proj_col_idx, record_indices, rel_version)

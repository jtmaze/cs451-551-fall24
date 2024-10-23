from typing import Literal

# TODO: For LRU cache, but large memory footprint
from collections import OrderedDict, namedtuple

from storage.bufferpool import Bufferpool
from storage.disk import Disk
from storage.rid import RID, rid_generator

from table import Record, MetaCol
from page import Page

RecordIndex = namedtuple("RecordIndex", ["page_id", "offset"])

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

    def get_record(self,
        rid: RID,
        proj_col_idx: list[Literal[0, 1]],
    ) -> Record | None:
        record_indices = self.page_dir[rid]

        return self.bufferpool.read(rid, proj_col_idx, record_indices)

    def get_page(self, rid, cols=None) -> Page | None:
        """OLD!!!!!
        """
        # Check buffer
        page = self.bufferpool.get_page(rid)

        # Go to persistent memory if not in buffer
        if page is None:
            page = self.disk.get_page(rid)
            if page:
                self.bufferpool.add_page(rid, page)

        return page

    def add_page(self, rid, page):
        self.disk.add_page(rid, page)

        self.bufferpool.add_page(rid, page)

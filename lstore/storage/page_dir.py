"""WARNING: PRETTY MUCH REPLACED BY buffer.py, WILL LIKELY DELETE SOON"""

from typing import Literal

from collections import OrderedDict
from collections import namedtuple

import itertools # Fast column projection with compress

from storage.disk import Disk
from storage.rid import RID, rid_generator

from table import Record, MetaCol
from page import Page


class PageDirectory:
    """
    """
    RecordIndex = namedtuple("RecordIndex", ["page_id", "offset"])
    PageID = int

    def __init__(self, num_columns: int, buffer_size: int) -> None:
        self.num_columns = num_columns
        self.buffer_size = buffer_size

        self.rid_gen = rid_generator()

        # In memory -------------------

        # Maps RIDs to (logical page ID, offset) pairs per column. Ordered by staleness
        self.page_dir: OrderedDict[RID, tuple[PageDirectory.RecordIndex]] = OrderedDict()

        # 2d list of pages for each column (including metadata)
        self.bufferpool: list[OrderedDict[PageDirectory.PageID, Page]] = [
            [OrderedDict()] for _ in range(MetaCol.COL_COUNT + num_columns)
        ]

        # Disk ------------------------

        self.disk = Disk()  # TODO: Support persistent memory

    def insert_record(self, record: Record) -> RID:
        rid: RID = next(self.rid_gen)

    def get_record(self, rid: RID, cols: list[Literal[0, 1]]) -> Record | None:
        # Get projection of (page ID, offset) pairs for cols from page_dir
        record_indices = itertools.compress(self.page_dir[rid], cols)

    def get_page(self, rid, cols=None) -> Page | None:
        """
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

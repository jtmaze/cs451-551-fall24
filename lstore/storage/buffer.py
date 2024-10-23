from typing import Literal, Callable

from collections import OrderedDict  # TODO: For LRU cache, but large memory footprint

import itertools # Fast column projection with compress

from storage.bufferpool import Bufferpool
from storage.disk import Disk
from storage.rid import RID, rid_generator

from table import Record
from page import Page


class Buffer:
    """
    """

    def __init__(self, num_columns: int) -> None:
        self.num_columns: int = num_columns

        self._rid_gen = rid_generator()

        # In memory -------------------

        # Maps RIDs to (logical page ID, offset) pairs per column. Ordered by staleness
        self.page_dir: OrderedDict[RID, list[Bufferpool.RecordIndex]] = OrderedDict()

        # Maps (logical page id->page) for each column (including metadata)
        self.bufferpool = Bufferpool(num_columns)

        # Disk ------------------------

        self.disk = Disk()  # TODO: Support persistent memory

    def insert_record(self, record: Record) -> RID:
        rid: RID = next(self._rid_gen)

        self.page_dir[rid] = self.bufferpool.write(rid, record.columns)

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

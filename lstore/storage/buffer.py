from typing import Literal

import time

from collections import OrderedDict  # TODO: For LRU cache, but large memory footprint
from collections import namedtuple

import itertools # Fast column projection with compress

from storage.bufferpool import Bufferpool
from storage.disk import Disk
from storage.rid import RID, rid_generator

from table import Record, MetaColumn
from page import Page


class Buffer:
    """
    """
    RecordIndex = namedtuple("RecordIndex", ["page_id", "offset"])

    def __init__(self, num_columns: int, buffer_size: int) -> None:
        self.num_columns = num_columns
        self._total_columns = MetaColumn.COLUMN_COUNT + num_columns

        self._rid_gen = rid_generator()

        # In memory -------------------

        # Maps RIDs to (logical page ID, offset) pairs per column. Ordered by staleness
        self.page_dir: OrderedDict[RID, tuple[Buffer.RecordIndex]] = OrderedDict()

        # Maps (page id->page) for each column (including metadata)
        self.bufferpool = Bufferpool(num_columns, buffer_size)

        # Disk ------------------------

        self.disk = Disk()  # TODO: Support persistent memory

    def insert_record(self, record: Record) -> RID:
        rid: RID = next(self._rid_gen)

        self.bufferpool.write(MetaColumn.INDIRECTION, 0)
        self.bufferpool.write(MetaColumn.RID, rid)
        self.bufferpool.write(MetaColumn.TIMESTAMP, time.now())
        self.bufferpool.write(MetaColumn.SCHEMA_ENCODING, 0) # 0b000...0

        for i in range(MetaColumn.COLUMN_COUNT, self._total_columns):
            self.bufferpool.write(i, record.columns[i])

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

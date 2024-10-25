"""
The buffer is responsible for all data in memory, and provides an interface
for operations such as insertion, reading, and deletion. It is managed through
RIDs that it allocates upon insertion. These RIDs are to be found by getting
them through an Index that maps query values to RIDs.

It contains a page directory for mapping RIDs to composite indices for records
located throughout columnar collections of pages.

It also contains a bufferpool where the pages actually live. The composite
indices are for accessing data via this bufferpool.
"""


from typing import Literal

from lstore.storage.bufferpool import Bufferpool
from lstore.storage.record_index import RecordIndex
from lstore.storage.disk import Disk
from lstore.storage.rid import RID

from lstore.storage.record import Record


class Buffer:
    """
    Buffer with page directory and bufferpool.

    :param table: Reference to parent table for things like num_columns
    """

    def __init__(self, table) -> None:
        self.table = table

        # In memory -------------------

        # Maps RIDs to (logical page ID, offset) pairs per column
        self.page_dir: dict[RID, list[RecordIndex]] = dict()

        # Maps (logical page id->page) for each column (including metadata)
        self.bufferpool = Bufferpool(self.table)

        # Disk ------------------------

        self.disk = Disk()  # TODO: Support persistent memory

    def insert_record(self, columns: tuple[int]) -> RID:
        """
        """
        rid: RID = RID.from_params(tombstone=0)

        # Write and insert record indices per each column into page dir
        self.page_dir[rid] = self.bufferpool.write(rid, columns)

        return rid

    def update_record(self, rid: RID, columns: tuple[int | None]):
        """
        Updates data record by adding a tail record with the data in columns.

        :param rid: Base RID
        :param columns: New data values
        """
        tail_rid: RID = RID.from_params(tombstone=0)

        # Update
        self.page_dir[tail_rid] = self.bufferpool.update(
            rid, tail_rid, columns)

    def get_record(
        self,
        rid: RID,
        proj_col_idx: list[Literal[0, 1]],
        rel_version: int
    ) -> Record | None:
        """
        :param rid: RID of base record to retrieve
        :param proj_col_idx: List of 0s or 1s indicating which columns to return
        :param rel_version: Relative version to return. 0 is newest, -<n> is old

        :return: Populated Record with data, or None if unsuccessful
        """
        return self.bufferpool.read(rid, proj_col_idx, rel_version)

    def delete_record(self, rid: RID):
        """
        Marks record as deleted by setting base record's indirection to special
        RID with tombstone == True
        """
        self.bufferpool.delete(rid)

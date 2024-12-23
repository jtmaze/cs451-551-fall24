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

from lstore.storage.buffer.bufferpool import Bufferpool

from lstore.storage.rid import RID

from lstore.storage.record import Record
from lstore import config


class Buffer:
    """
    Buffer for in memory management.

    :param table: Reference to parent table
    """

    def __init__(self, table) -> None:
        self.table = table

        # In memory -------------------

        # Maps (logical page id->page) for each column (including metadata)
        self.bufferpool = Bufferpool(self.table)

    def insert_record(self, columns: tuple[int]) -> RID:
        """
        Creates a new RID and inserts a new record with the given data.

        :param columns: New data values

        :return: The created RID to be stored in the index
        """
        return self.bufferpool.write(columns)

    def update_record(self, rid: RID, columns: tuple[int | None]):
        """
        Updates data record by adding a tail record with the data in columns.

        :param rid: Base RID
        :param columns: New data values
        """
        # Update and save to page directory (for bufferpool to find)
        self.bufferpool.update(rid, 0, columns)

    def get_record(
        self,
        rid: RID,
        proj_col_idx: list[Literal[0, 1]],
        rel_version: int
    ) -> Record:
        """
        :param rid: RID of base record to retrieve
        :param proj_col_idx: List of 0s or 1s indicating which columns to return
        :param rel_version: Relative version to return. 0 is latest, -<n> are prev

        :return: Populated Record associated with given RID
        """
        return self.bufferpool.read(rid, proj_col_idx, rel_version)

    def delete_record(self, rid: RID):
        """
        Marks record as deleted by setting base record's indirection to special
        RID with tombstone == True

        :param rid: The RID of the record to delete
        """
        # Update and save to page directory (for bufferpool to find)
        self.bufferpool.update(
            rid, 1, tuple(None for _ in range(self.table.num_columns)))
        # for rollback(transaction.py)

    def revert_update(self, rid: RID):
        """
        Restores a deleted record by resetting its tombstone flag.
        :param rid: The RID of the record to restore
        """
        try:
            self.bufferpool.restore(rid)
            print(f"Record {rid} successfully restored.")
        except Exception as e:
            print(f"Failed to restore record {rid}: {e}")
        
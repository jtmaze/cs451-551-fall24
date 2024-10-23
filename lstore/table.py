from typing import Literal

from enum import Enum

import config

from lstore.index import Index
from storage.buffer import Buffer


class MetaCol(Enum):
    INDIR = 0      # Base: RID of latest tail; Tail: RID of prev
    RID = 1        # Record ID (and index/location/hashable in page directory)
    TIMESTAMP = 2  # Timestamp for both base and tail record
    SCHEMA = 3     # Bits representing cols, 1s where updated

    COL_COUNT = 4  # Number of metadata columns


class Record:
    """
    Data record (not metadata). RID gets populated by page directory.
    :param rid:
    :param key:
    :param columns: Array of data values
    """

    def __init__(self, key, columns, rid=None):
        self.key = key
        self.columns = columns

        self.rid = rid


class Table:
    """
    :param name:         # Table name
    :param num_columns:  # Number of DATA columns (all columns are integer)
    :param key:          # Index of table key in columns (ie primary key, ex 2 if 3rd col)
    """

    def __init__(self, name: str, num_columns: int, key: int):
        if key >= num_columns:
            raise IndexError("Key index is greater than the number of columns")

        self.name = name
        self.num_columns = num_columns
        self.key = key

        # Given RID, returns records (checks bufferpool before disk)
        self.buffer = Buffer(self)

        # Index for faster querying on primary key and possibly other columns
        self.index = Index(self)

    def __merge(self):
        print("merge is happening")

    def insert(self, record: Record) -> int:
        """
        Given a new Record with a key and columns filled, attempts to insert
        the record through the page directory and populates the RID field.

        Returns an error code as an integer:
            0: No error
            1: Unspecified error
        """
        try:
            # Insert a record, directory will return its new RID value
            record.rid = self.buffer.insert_record(record)
        except Exception as e:
            # This catch-all error should be last
            print(f"Error inserting record '{record.key}'")
            return 1

        # Optionally update the index (this could be more sophisticated based on your needs)
        self.index.create_index(self.key, record.rid)

        return 0

    def select(self, search_key, search_key_idx, proj_col_idx: list[Literal[0, 1]]) -> list[Record]:
        """
        Select records based on the primary key. Use the index for fast lookup.
        """
        # Get rid (point query) or rids (range query) via index
        rid_list = self.index.locate(self.key, search_key)

        result = []
        for rid in rid_list:
            try:
                record: Record = self.buffer.get_record(rid, proj_col_idx)
            except KeyError as e:
                print(f"Failed to find rid={rid}")

            result.append(record)

        return result

    def update(self, rid, updated_values):
        """
        Updates the record with the given RID.
        """
        page = self.buffer.get_page(rid)

        if page:
            page.update(rid, updated_values)

    def delete(self, rid):
        """
        Deletes the record with the given RID by marking it invalid in the bufferpool.
        """
        page = self.buffer.get_page(rid)
        if page:
            page.invalidate(rid)
            self.buffer.add_page(rid, page)

    def __del__(self):
        """
        Table destructor. Writes pages only in memory to disk.
        """
        # TODO: Write buffer pages to disk
        pass

from lstore.index import Index
from time import time

import config

from lstore.page import Page
from storage.page_dir import PageDirectory

INDIRECTION_COLUMN = 0  # Base: RID of latest tail; Tail: RID of prev
RID_COLUMN = 1  # Record ID (and index/location/hashable in page directory)
TIMESTAMP_COLUMN = 2  # Timestamp for both base and tail record
SCHEMA_ENCODING_COLUMN = 3  # Bits representing cols, 1s where updated


class Record:
    """
    Data record (not metadata). RID gets populated by page directory.
    :param rid:
    :param key:
    :param columns:
    """

    def __init__(self, key, columns):
        self.rid = None

        self.key = key
        self.columns = columns


class Table:
    """
    :param name:         # Table name
    :param num_columns:  # Number of columns: all columns are integer
    :param key:          # Index of table key in columns (ie primary key, ex 2 if 3rd col)
    """

    def __init__(self, name: str, num_columns: int, key: int):
        if key >= num_columns:
            raise IndexError("Key index is greater than the number of columns")

        self.name = name
        self.num_columns = num_columns
        self.key = key

        # Given RID, returns records (checks bufferpool first)
        self.page_directory = PageDirectory(
            num_columns=num_columns,
            buffer_size=config.MAX_BUFFER_SIZE,  # None -> uncapped
        )

        # Index for faster querying on primary key and possibly other columns
        self.index = Index(self)

    def __merge(self):
        print("merge is happening")

    def insert(self, record: Record) -> int:
        """
        Given a new Record with a key and columns filled, populates the rid
        field and stores the record in pages accessed through the updated
        directory.

        Returns an error code as an integer:
            0: No error
            1: Unspecified error
            2: Duplicate RID
        """
        try:
            # Insert a record, directory will populate the RID field (and return it)
            rid = self.page_directory.insert_record(record)
        except KeyError as a:
            print(f"Can't insert record '{record.rid}' as it already exists")
            return 2
        except Exception as e:
            # This catch-all error should be last
            print(f"Error inserting record '{record.rid}'")
            return 1

        # Optionally update the index (this could be more sophisticated based on your needs)
        self.index.create_index(self.key, rid)

        return 0

    def select(self, key, columns=None) -> list[Record]:
        """
        Select records based on the primary key. Use the index for fast lookup.
        """
        rid_list = self.index.locate(self.key, key)

        result = []
        for rid in rid_list:
            try:
                record: Record = self.page_directory.get_record(rid, columns)
            except KeyError as e:
                print(f"Failed to find rid={rid}")

            result.append(record)

        return result

    def update(self, rid, updated_values):
        """
        Updates the record with the given RID.
        """
        page = self.page_directory.get_page(rid)

        if page:
            page.update(rid, updated_values)

    def delete(self, rid):
        """
        Deletes the record with the given RID by marking it invalid in the bufferpool.
        """
        page = self.page_directory.get_page(rid)
        if page:
            page.invalidate(rid)
            self.page_directory.add_page(rid, page)

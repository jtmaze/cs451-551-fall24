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
    Data record (not metadata)
    :param rid:      # Record ID
    :param key:      # Primary key value
    :param columns:  # Data in record's columns (including key)
    """

    def __init__(self, rid, key, columns):
        self.rid = rid
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
            buffer_size=config.MAX_BUFFER_SIZE, # None -> uncapped
        )

        # Index for faster querying on primary key and possibly other columns
        self.index = Index(self)

    def __merge(self):
        print("merge is happening")

    def insert(self, record: Record):
        """
        Insert a new record into the table. Allocate a new page if needed and
        store the record in the bufferpool/disk through the directory.
        """
        rid = self._generate_rid()
        page = self.page_directory.get_page(rid)

        # If the page does not exist in the bufferpool, create a new page
        if page is None:
            page = Page()

        # Write the new record to the page
        try:
            # Assuming the key is the value being written for simplicity
            page.write(record.key)
        except Exception as e:
            print(f"Error inserting record: {e}")
            return

        # Add the new page to the directory
        self.page_directory.add_page(rid, page)

        # Optionally update the index (this could be more sophisticated based on your needs)
        self.index.create_index(self.key)

    def select(self, key):
        """
        Select records based on the primary key. Use the index for fast lookup.
        """
        rid_list = self.index.locate(self.key, key)

        result = []
        for rid in rid_list:
            page = self.page_directory.get_page(rid)

            if page:
                result.append(page.read(rid))

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

    # Helpers ---------------------

    def _generate_rid(self):
        """
        A simple RID generator.
        """
        return int(time() * 1000)  # Using the current time as a RID generator

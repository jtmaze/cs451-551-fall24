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
    """Data record (not metadata)
    :param rid:      # Record ID
    :param key:      # Primary key value
    :param columns:  # Data in record's columns (including key)
    """

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Bufferpool:
    """
    A simple bufferpool that uses a hash table to store pages in memory,
    using RIDs (Record IDs) as keys.
    """
    def __init__(self, max_size=None):
        # Max buffer size (optional), could be useful for future use in eviction policy
        self.max_size = max_size
        self.page_directory = {}

    def get_page(self, rid):
        """
        Retrieves a page using the given RID from the bufferpool.
        """
        return self.page_directory.get(rid, None)

    def add_page(self, rid, page):
        """
        Adds a page to the bufferpool and associates it with the given RID.
        """
        self.page_directory[rid] = page

    def remove_page(self, rid):
        """
        Removes a page from the bufferpool.
        """
        if rid in self.page_directory:
            del self.page_directory[rid]

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

        # Given RID, returns records
        self.page_directory = PageDirectory(
            num_columns=num_columns,
            buffer_size=None, #config.MAX_BUFFER_SIZE
        )

        self.index = Index(num_columns)
        # Page Directory to keep track of RIDs and pages

        self.page_directory = PageDirectory(
            num_columns=num_columns,
            buffer_size=None,  # config.MAX_BUFFER_SIZE
        )

        # Index for faster querying on primary key and possibly other columns
        self.index = Index(self)

        # Instantiate a bufferpool for managing in-memory pages
        self.bufferpool = Bufferpool()

    def __merge(self):
        print("merge is happening")

    def insert(self, record: Record):
        """
        Insert a new record into the table. Allocate a new page if needed and
        store the record in the bufferpool.
        """
        rid = self.generate_rid()
        page = self.bufferpool.get_page(rid)

        # If the page does not exist in the bufferpool, create a new page
        if page is None:
            page = Page()

            # Write the new record to the page
        try:
            page.write(record.key)  # Assuming the key is the value being written for simplicity
        except Exception as e:
            print(f"Error inserting record: {e}")
            return

            # Add the page back to the bufferpool
        self.bufferpool.add_page(rid, page)

        # Optionally update the index (this could be more sophisticated based on your needs)
        self.index.create_index(self.key)

    def select(self, key):
        """
        Select records based on the primary key. Use the index for fast lookup.
        """
        rid_list = self.index.locate(self.key, key)

        result = []
        for rid in rid_list:
            page = self.bufferpool.get_page(rid)
            if page:
                result.append(page.read(rid))

        return result

    def update(self, rid, updated_values):
        """
        Updates the record with the given RID.
        """
        page = self.bufferpool.get_page(rid)
        if page:
            # Perform the update
            page.update(rid, updated_values)
            self.bufferpool.add_page(rid, page)

    def delete(self, rid):
        """
        Deletes the record with the given RID by marking it invalid in the bufferpool.
        """
        page = self.bufferpool.get_page(rid)
        if page:
            page.invalidate(rid)
            self.bufferpool.add_page(rid, page)

    def generate_rid(self):
        """
        A simple RID generator.
        """
        return int(time() * 1000)  # Using the current time as a RID generator
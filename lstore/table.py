"""
Tables in the database. Contains important descriptive attributes such as the
index of the primary key and number of data columns. Also manages in-memory
buffer and indices for performant querying.
"""

from typing import Literal

from lstore.index import Index
from lstore.storage.buffer.buffer import Buffer
from lstore.storage.record import Record
from lstore.storage.rid import RID
from lstore.storage.meta_col import MetaCol

from lstore.index_types.index_config import IndexConfig

from lstore import config


class Table:
    """
    Database table with buffer and query indices.

    :param name:         # Table name
    :param num_columns:  # Number of DATA columns (all columns are integer)
    :param key:          # Index of table key in columns (ie primary key, ex 2 if 3rd col)
    """

    def __init__(self, 
        name: str,
        num_columns: int, 
        key: int,
        index_config: IndexConfig
    ):
        if key >= num_columns:
            raise IndexError("Key index is greater than the number of columns")

        self.name = name
        self.key = key

        self.num_columns = num_columns
        self.num_total_cols = num_columns + len(MetaCol)

        # Given RID, returns records (checks bufferpool before disk)
        self.buffer = Buffer(self)

        # Index for faster querying on primary key and possibly other columns
        self.index = Index(self, key, num_columns, index_config)

    def __merge(self):
        print("merge is happening")

    def insert(self, columns: tuple[int]):
        """
        Inserts a new record with the given data in columns.

        Raises an exception if something went wrong.

        :param columns: New data values
        """
        try:
            # Insert a record, buffer will return its new RID
            rid = self.buffer.insert_record(columns)
        except Exception as e:
            print(f"Error inserting record '{columns}'")
            raise  # Re-raise exception error

        # Update primary key's index
        self.index.insert_val(self.key, columns[self.key], rid)

        # TODO: Update other indices
        pass

    def select(
        self,
        search_key: int,
        search_key_idx: int,
        proj_col_idx: list[Literal[0, 1]],
        rel_version: int = 0  # Default to newest tail (lastest version)
    ) -> list[Record]:
        """
        Select records based on the primary key. Use the index for fast lookup.

        :param search_key: Value to search on in index column
        :param search_key_idx: Index of column to search
        :param proj_col_idx: Data column indices that will be returned
        :param rel_version: Relative record version. 0 is latest, -<n> are prev

        :return: A list of Records for each projected column
        """
        # Get rid (point query) or rids (range query) via index
        rid_list = self.index.locate(search_key_idx, search_key)

        records = []
        for rid in rid_list:
            try:
                records.append(
                    self.buffer.get_record(rid, proj_col_idx, rel_version)
                )
            except KeyError:
                pass
            except Exception as e:
                if config.DEBUG_PRINT:
                    print(f"Failed to find rid={rid.rid}")

        return records
    
    def select_range(
        self,
        start_range: int,
        end_range: int,
        search_key_idx: int,
        proj_col_idx: list[Literal[0, 1]],
        rel_version: int = 0  # Default to newest tail (lastest version)
    ) -> list[Record]:
        rid_list = self.index.locate_range(start_range, end_range, search_key_idx)
        records = []
        for rid in rid_list:
            try:
                records.append(
                    self.buffer.get_record(rid, proj_col_idx, rel_version)
                )
            except Exception as e:
                print(f"Failed to find rid={rid}")

        return records


    def update(self, rid: RID, columns: tuple[int]):
        """
        Updates the record with the given RID. This updates the base record's
        schema encoding and indirection pointer to point to the latest tail
        record, but does not alter its data.

        :param rid: RID of base record to update
        :param columns: New data values
        """
        self.buffer.update_record(rid, columns)

    def delete(self, rid: RID):
        """
        Deletes the record with the given RID by marking it invalid

        :param rid: RID of record to 'delete'
        """
        self.buffer.delete_record(rid)


    def flush_pages(self):
        """
        Writes all dirty pages in the buffer pool to disk and marks them as clean.
        """
        for col_index, column_pages in enumerate(self.buffer.bufferpool.pages):
            for page_id, page in column_pages.items():
                if page.is_dirty:
                    # disk write function
                    # something like: self.buffer.disk.write_page(page_id, page.data)
                    # After writing to disk, mark page as clean
                    page.is_dirty = False
                    print(f"Flushed page {page_id} in column {col_index} to disk.")

    # Clear tables in memory
    def clear(self):
        """
        Clears in-memory pages from the table, making sure to flush all dirty pages first.
        """
        self.flush_pages()  # Ensure all data is safely written to disk
            # Clear the page directory mapping RIDs to in-memory pages with something like:
            #self.buffer.page_dir.clear()
            #self.buffer.bufferpool.pages = [dict() for _ in range(self.buffer.bufferpool.total_columns)]
        print("Cleared all in-memory pages from the table.")

    def __del__(self):
        """
        Table destructor. Writes pages only in memory to disk.
        """
        # TODO: Write buffer pages to disk
        pass


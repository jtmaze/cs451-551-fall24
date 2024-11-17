"""
Tables in the database. Contains important descriptive attributes such as the
index of the primary key and number of data columns. Also manages in-memory
buffer and indices for performant querying.
"""

from typing import Literal

import concurrent.futures

from lstore.index import Index
from lstore.storage.buffer.buffer import Buffer
from lstore.storage.record import Record
from lstore.storage.rid import RID
from lstore.storage.meta_col import MetaCol
from lstore.storage.disk import Disk
from lstore.storage.buffer.merge_mgr import MergeManager

from lstore.index_types.index_config import IndexConfig

from lstore import config


class Table:
    """
    Database table with buffer and query indices.

    :param name:         # Table name
    :param num_columns:  # Number of DATA columns (all columns are integer)
    :param key:          # Index of table key in columns (ie primary key, ex 2 if 3rd col)
    """
    class DuplicateKeyError(Exception):
        """Custom exception for duplicate primary keys."""
        pass

    class MissingKeyError(Exception):
        """Custom exception for missing primary keys."""
        pass

    def __init__(self, 
        name: str,
        num_columns: int, 
        key: int,
        db_path: str,
        index_config: IndexConfig,
        delete_tracker: list[int] = None
    ):
        if key >= num_columns:
            raise IndexError("Key index is greater than the number of columns")

        self.name = name
        self.key = key

        self.db_path = db_path

        self.num_columns = num_columns
        self.num_total_cols = num_columns + len(MetaCol)

        # Index for faster querying on primary key and possibly other columns
        # Creates a index for every column
        self.index = Index(self, 0, num_columns, index_config)

        # Given RID, returns records (checks bufferpool before disk)
        self.buffer = Buffer(self)

        self.disk: Disk = Disk(self)

        self.num_updates = 0
        self.merge_threshold = config.MERGE_UPDATE_THRESHOLD
        self.merge_mgr: MergeManager = MergeManager(self)

        if delete_tracker is None:
            self.delete_tracker = set()
        else:
            self.delete_tracker = set(delete_tracker)

    def reconstruct_index(self, index_cols: list[int]):
        """
        Rebuilds the index from the existing data in the table's base pages.
        """
        print(f"Reconstructing index for table '{self.name}'...")
        self.index.clear()  # Clear existing indices

        # Scan all base records and insert them into the index
        records_by_column = {i: [] for i in range(self.num_columns)}

        for rid, record_values in self.disk.scan_base_records(index_cols):
            # for col_index, value in enumerate(record_values):
            for col_index, value in zip(index_cols, record_values):
                records_by_column[col_index].append((value, rid))

        # Bulk insert into indices
        for col_index, records in records_by_column.items():
            self.index.bulk_insert(col_index, records)

        print(f"Index reconstruction completed for table '{self.name}'.")

    # Core operations ----------------------

    def insert(self, columns: tuple[int]):
        """
        Inserts a new record with the given data in columns.

        Raises an exception if something went wrong.

        :param columns: New data values
        """
        try:
            # Check if primary key exists (raises error if not)
            self._validate_primary_key_insert(columns)

            # Insert a record, buffer will return its new RID
            rid = self.buffer.insert_record(columns)

            # Update indexes
            for col in self.index.index_cols:
                self.index.insert_val(
                    col, columns[col], rid, is_prim_key=(col == self.key))
        except Table.DuplicateKeyError as e:
            print(e)
        except Exception as e:
            print(f"Error inserting record '{columns}'")
            raise  # Re-raise exception error

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
        rid_list = self.index.locate_range(start_range, end_range, search_key_idx, is_prim_key = (search_key_idx == self.key))
        records = []
        for rid in rid_list:
            try:
                records.append(
                    self.buffer.get_record(rid, proj_col_idx, rel_version)
                )
            except Exception as e:
                print(f"Failed to find rid={rid}")

        return records

    def update(self, rid: RID, columns: tuple[int], primary_key):
        """
        Updates the record with the given RID. This updates the base record's
        schema encoding and indirection pointer to point to the latest tail
        record, but does not alter its data.

        :param rid: RID of base record to update
        :param columns: New data values
        """
        try:
            self._validate_primary_key_update(primary_key)
            self.buffer.update_record(rid, columns)

            self.num_updates += 1
            if self.num_updates >= self.merge_threshold:
                self.merge()
        except Table.DuplicateKeyError as e:
            print(e)

    def delete(self, rid: RID, primary_key):
        """
        Deletes the record with the given RID by marking it invalid

        :param rid: RID of record to 'delete'
        """
        try:
            self._validate_primary_key_delete(primary_key)

            self.buffer.delete_record(rid)
            self.delete_tracker.add(primary_key)
        except Table.DuplicateKeyError as e:
            print(e)

    # Utility ----------------------

    def flush_pages(self):
        """
        Writes all dirty pages in the buffer pool to disk and marks them as clean.
        """
        self.buffer.bufferpool.flush_to_disk()

    def clear(self):
        """
        Clears in-memory pages from the table, making sure to flush all dirty pages first.
        """
        # Ensure all data is safely written to disk
        self.flush_pages()

    def merge(self):
        # self.flush_pages()

        print("Running merge...")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            merge_future = executor.submit(self.merge_mgr.merge)
        
            merge_future.add_done_callback(
                self.merge_mgr.finalize_merge)        

        self.num_updates = 0

    # Helpers ------------------------------------------------

    def _validate_primary_key_insert(self, columns):
        primary_key = columns[self.key]

        if self.index.locate(self.key, primary_key):
            if primary_key in self.delete_tracker:
                self.delete_tracker.discard(primary_key)
                return

            e = f"A record with key {primary_key} already exists, skipping insert."
            raise Table.DuplicateKeyError(e)

    def _validate_primary_key_update(self, primary_key):
        if not self.index.locate(self.key, primary_key):
            e = f"No record with key {primary_key} exists, skipping update."
            raise Table.MissingKeyError(e)

    def _validate_primary_key_delete(self, primary_key):
        if not self.index.locate(self.key, primary_key):
            e = f"No record with key {primary_key} exists, skipping delete."
            raise Table.MissingKeyError(e)
        
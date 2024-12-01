from typing import Literal

from lstore.table import Table, Record
from lstore.index import Index

from lstore import config


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table: Table = table
        self.transaction_context = None  # To track if running within a transaction

    def delete(self, primary_key) -> bool:
        """
        # internal Method
        # Read a record with specified RID
        # Returns True upon successful deletion
        # Return False if record doesn't exist or is locked due to 2PL
        """
        try:
            # Locate the RID via the primary key
            rid_list = self.table.index.locate(self.table.key, primary_key)
            if not rid_list:
                return False  # Record not found

            rid = rid_list[0]
            # Log state for rollback if in a transaction
            if self.transaction_context:
                record = self.table.buffer.get_record(
                    rid, [1] * self.table.num_columns, rel_version=0
                )
                self.transaction_context.log_delete(rid, record)

            self.table.delete(rid, primary_key)
            return True
        except Exception as e:
            self._print_error(e)
            return False

    def insert(self, *columns) -> bool:
        """
        # Insert a record with specified columns
        # Return True upon succesful insertion
        # Returns False if insert fails for whatever reason
        """
        try:
            # Insert the record into the table
            self.table.insert(columns)
            return True
        except Exception as e:
            self._print_error(e)
            return False

    def select(
        self,
        search_key,
        search_key_index,
        projected_columns_index: list[Literal[0, 1]]
    ) -> list[Record] | bool:
        """
        # Read matching record with specified search key
        # :param search_key: the value you want to search based on
        # :param search_key_index: the column index you want to search based on
        # :param projected_columns_index: what columns to return. array of 1 or 0 values.
        # Returns a list of Record objects upon success
        # Returns False if record locked by TPL
        # Assume that select will never be called on a key that doesn't exist
        """
        return self._select_core(search_key, search_key_index, projected_columns_index)

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        """
        # Read matching record with specified search key
        # :param search_key: the value you want to search based on
        # :param search_key_index: the column index you want to search based on
        # :param projected_columns_index: what columns to return. array of 1 or 0 values.
        # :param relative_version: the relative version of the record you need to retreive.
        # Returns a list of Record objects upon success
        # Returns False if record locked by TPL
        # Assume that select will never be called on a key that doesn't exist
        """
        return self._select_core(search_key, search_key_index, projected_columns_index, relative_version)
    
    def select_version_range(self, start_range, end_range, search_key_index, projected_columns_index, relative_version):
        return self._select_core_range(start_range, end_range, search_key_index, projected_columns_index, relative_version)

    def update(self, primary_key, *columns: tuple[None | int]) -> bool:
        """
        # Update a record with specified key and columns
        # :param primary_key: Primary key value, not index
        # :param columns: Tuple of new values (or None if unchanged)
        # Returns True if update is succesful
        # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
        """
        try:
            # Locate the RID via the primary key
            rid_list = self.table.index.locate(self.table.key, primary_key)
            if not rid_list:
                return False  # Record not found

            rid = rid_list[0]

            # Log state for rollback if in a transaction
            if self.transaction_context:
                record = self.table.buffer.get_record(
                    rid, [1] * self.table.num_columns, rel_version=0
                )
                self.transaction_context.log_update(rid, record.columns)

            self.table.update(rid, columns, primary_key)
            return True
        except Exception as e:
            self._print_error(e)
            return False

    def sum(self, start_range, end_range, aggregate_column_index):
        """
        :param start_range: int         # Start of the key range to aggregate 
        :param end_range: int           # End of the key range to aggregate 
        :param aggregate_columns: int  # Index of desired column to aggregate
        # this function is only called on the primary key.
        # Returns the summation of the given range upon success
        # Returns False if no record exists in the given range
        """
        return self._sum_core(start_range, end_range, aggregate_column_index)

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        """
        :param start_range: int         # Start of the key range to aggregate 
        :param end_range: int           # End of the key range to aggregate 
        :param aggregate_columns: int  # Index of desired column to aggregate
        :param relative_version: the relative version of the record you need to retreive.
        # this function is only called on the primary key.
        # Returns the summation of the given range upon success
        # Returns False if no record exists in the given range
        """
        return self._sum_core(start_range, end_range, aggregate_column_index, relative_version)

    def increment(self, key, column):
        """
        increments one column of the record
        this implementation should work if your select and update queries already work
        :param key: the primary of key of the record to increment
        :param column: the column to increment
        # Returns True is increment is successful
        # Returns False if no record matches key or if target record is locked by 2PL.
        """
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    # Helpers -------------------------

    def _select_core(self, search_key, search_key_index, projected_columns_index, relative_version=0):
        """
        Core select functionality for use by select and select_version.
        """
        try:
            # Get projected list of records
            records = self.table.select(
                search_key,
                search_key_index,
                projected_columns_index,
                relative_version,
            )

            return records
        except Exception as e:
            self._print_error(e)
            return False
        
    def _select_core_range(self, start_range, end_range, search_key_index, projected_columns_index, relative_version=0):
        try: 
            records = self.table.select_range(
                start_range,
                end_range,
                search_key_index,
                projected_columns_index,
                relative_version
            )
            
            return records
        except Exception as e:
            self._print_error(e)
            return False
        

    def _sum_core(self, start_range, end_range, aggregate_column_index, relative_version=0):
        """
        Core summation functionality for use by sum and sum_version.

        TODO: REMOVE THAT WE GRAB WHOLE RECORDS WHEN CHECKING VALS
        """
        try:
            total_sum = 0
            records = self.select_version_range(
                    start_range, end_range, self.table.key, [1] * self.table.num_columns, relative_version)
            if records:
                for record in records:
                    total_sum += record.columns[aggregate_column_index]
            return total_sum
        except Exception as e:
            self._print_error(e)
            return False
        
    def count(self, start_range, end_range, column_index, relative_version = 0):
        """
        Counts number of records with column value between start_range and end_range. 
        set start range and end range equal to eachother if you just want num records with column value equal to that one number
        """
        proj_col_idx = [0] * self.table.num_columns
        proj_col_idx[column_index] = 1
        try:
            total_count = 0
            records = self.select_version_range(start_range, end_range, column_index, proj_col_idx, relative_version)
            if records:
                total_count = len(records)
            return total_count
        except Exception as e:
            self._print_error(e)
            return False


    @staticmethod
    def _print_error(err):
        if config.PRINT_ERRORS:
            print(f"{type(err)}: {err}")

    def set_transaction_context(self, transaction):
        """
        Sets the transaction context for the query.
        This ensures that all operations log changes for rollback if necessary.
        """
        self.transaction_context = transaction
"""
A data structure holding indices for various columns of a table.

Key column should be indexd by default, other columns can be indexed through 
this object.

Indices are usually B-Trees, but other data structures can be used as well.
"""
#from data_structures.btree import BTree

from lstore.storage.rid import RID

class DictIndex:
    def __init__(self):
        self.data = dict()

    def get(self, val) -> RID:
        return self.data[val]

    def get_range(self):
        raise NotImplementedError()

    def insert(self, val, rid):
        self.data[val] = rid

    def delete(self, val):
        del self.data[val]

class Index:

    def __init__(self, table, key, num_columns):
        self.table = table

        # One index for each table. All our empty initially.
        self.indices = [None for _ in range(num_columns)]

        # Populate the index for the primary key
        self.create_index(key)

    def locate(self, column, value):
        """
        # returns the location of all records with the given value on column "column"
        """
        if self.indices[column] is None:
            # If no index exists, return an empty list
            return []

        # If an index exists, use it to look up the RIDs
        return self.indices[column].get(value, [])

    def locate_range(self, begin, end, column):
        """
        # Returns the RIDs of all records with values in column "column" between "begin" and "end"
        """
        if self.indices[column] is None:
            return []

        # Collect all RIDs for values within the specified range
        result = []
        for value in range(begin, end + 1):  # Assuming integer range for simplicity
            result.extend(self.indices[column].get(value, []))
        return result

    def create_index(self, column_number):
        """
        # optional: Create index on specific column
        """
        if self.indices[column_number] is None:
            # Create a new dictionary to serve as the index for this column
            self.indices[column_number] = DictIndex()

            self._populate_index(column_number)

    def drop_index(self, column_number):
        """
        # optional: Drop index of specific column
        """
        if self.indices[column_number] is not None:
            self.indices[column_number] = None

    def insert_val(self, col_number, val, rid):
        if self.indices[col_number] is not None:
            self.indices[col_number].insert(val, rid)

    # Helper ---------------------

    def _populate_index(self, col_number):
        """Goes through already data in column and populates index."""
        pass
        # Assuming that the table is accessible through a reference (e.g., self.table)
        # for rid, record in enumerate(self.table_data()):  # Replace with actual record retrieval method
        #     column_value = record.columns[column_number]
        #     if column_value in self.indices[column_number]:
        #         self.indices[column_number][column_value].append(rid)
        #     else:
        #         self.indices[column_number][column_value] = [rid]

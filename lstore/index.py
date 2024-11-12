"""
A data structure holding indices for various columns of a table.

Key column should be indexd by default, other columns can be indexed through 
this object.

Indices are usually B-Trees, but other data structures can be used as well.
"""

from lstore.index_types.index_config import IndexConfig

from lstore.index_types.bptree import BPTreeIndex

from lstore.index_types.dict_index import DictIndex

class Index:

    def __init__(self, table, key, num_columns, index_config):
        self.table = table
        self.index_config: IndexConfig = index_config

        # One index for each table. All our empty initially.
        self.indices = [None for _ in range(num_columns)]

        # Populate the index for the primary key
        for i in range(num_columns):
            self.create_index(i)

    def locate(self, column, value):
        """
        # returns the location of all records with the given value on column "column"
        """
        if self.indices[column] is None:
            # If no index exists, return an empty list
            return []

        # If an index exists, use it to look up the RIDs
        return self.indices[column].get(value)

    def locate_range(self, begin, end, column, is_prim_key = False):
        """
        # Returns the RIDs of all records with values in column "column" between "begin" and "end"
        """
        if self.indices[column] is None:
            return []

        result = []
        if is_prim_key: 
            result.extend(self.indices[column].get_range_key(begin, end))
        else: 
            result.extend(self.indices[column].get_range_val(begin, end))
        # Collect all RIDs for values within the specified range
        
        return result

    def create_index(self, column_number):
        """
        # optional: Create index on specific column
        """
        cfg = self.index_config

        if self.indices[column_number] is None:
            # Create a Index to serve as the index for this column
            if cfg.index_type == BPTreeIndex:
                self.indices[column_number] = cfg.index_type(cfg.node_size)
            else:
                self.indices[column_number] = cfg.index_type()

            self._populate_index(column_number)

    def drop_index(self, column_number):
        """
        # optional: Drop index of specific column
        """
        if self.indices[column_number] is not None:
            self.indices[column_number] = None

    def insert_val(self, col_number, val, rid, is_prim_key = False):
        if self.indices[col_number] is not None and (is_prim_key or self.index_config.index_type == BPTreeIndex):
            self.indices[col_number].insert(val, rid)
        elif self.indices[col_number] is not None and self.index_config.index_type == DictIndex:
            self.indices[col_number].insert(rid, val)


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


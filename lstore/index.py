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
        self.key = key
        self.index_config: IndexConfig = index_config

        # List of column numbers for indexes (populated by self.create_index)
        self.index_cols = []

        # One index for each table. All our empty initially.
        self.indices = [None for _ in range(num_columns)]

        # Populate the indexes for specified columns (or all if unspecified)
        if index_config.index_cols is not None:
            index_cols = set(index_config.index_cols)
            index_cols.add(key)

            for col_idx in index_cols:
                self.create_index(col_idx)
        else:
            for i in range(num_columns):
                self.create_index(i)

    def locate(self, column, value):
        """
        # returns the location of all records with the given value on column "column"
        """
        # If no index, perform linear scan
        if self.indices[column] is None:
            key_rids_pairs = self.indices[self.key].scan_all()
            proj_idx = [1 if i == column else 0 for i in range(self.table.num_columns)]

            rids = []
            for primary_key, rid in key_rids_pairs:
                vals = self.table.select(primary_key, self.table.key, proj_idx)[0]
                if vals.columns[0] == value:
                    rids.append(rid)

            return rids

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

        self.index_cols.append(column_number)

    def drop_index(self, column_number):
        """
        # optional: Drop index of specific column
        """
        if self.indices[column_number] is not None:
            self.indices[column_number] = None

            self.index_cols.remove(column_number)

    def insert_val(self, col_number, val, rid, is_prim_key = False):
        index = self.indices[col_number]

        # if index is not None and (is_prim_key or isinstance(index, BPTreeIndex)):
        if index is not None:
            index.insert(val, rid)
        else:
            raise TypeError(f"Inserting value {val} into nonexistent index (col {col_number})")
        
    def update_val(self, col_number, old_val, new_val, rid):
        index = self.indices[col_number]

        if index is not None:
            index.update(old_val, new_val, rid)
        else:
            raise TypeError(f"Updating value {old_val}->{new_val} for nonexistent index (col {col_number})")



    # Helper ---------------------

    def _populate_index(self, col_number):
        """Goes through already data in column and populates index."""
        key_rids_pairs = self.indices[self.key].scan_all()

        index = self.indices[col_number]
        proj_idx = [1 if i == col_number else 0 for i in range(self.table.num_columns)]

        for primary_key, rid in key_rids_pairs:
            vals = self.table.select(primary_key, self.table.key, proj_idx)[0]
            index.insert(vals.columns[0], rid)

    #new_______________for reconstructing index
    def clear(self):
        """
        Clears all indices for this table.
        """
        for index in self.indices:
            if index is not None:
                index.clear()

    def bulk_insert(self, col_number, records):
        """
        Bulk insert records into the index for a specific column.
        :param col_number: Column number to index.
        :param records: List of (value, RID) tuples.
        """
        if self.indices[col_number] is None:
            return

        for value, rid in records:
            self.insert_val(col_number, value, rid, (col_number == self.key))

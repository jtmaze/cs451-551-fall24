"""
A data structure holding indices for various columns of a table.

Key column should be indexd by default, other columns can be indexed through 
this object.

Indices are usually B-Trees, but other data structures can be used as well.
"""

from lstore.data_structures.bptree import BPTree

from lstore.storage.rid import RID

class DictIndex:
    def __init__(self):
        self.data = dict()

    def get(self, val) -> list[RID]:
        output = self.data.get(val, None)

        if output is None:
            return []
        
        return [output]

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
        return self.indices[column].get(value)

    def locate_range(self, begin, end, column):
        """
        # Returns the RIDs of all records with values in column "column" between "begin" and "end"
        """
        if self.indices[column] is None:
            return []

        # Collect all RIDs for values within the specified range
        result = []
        for value in range(begin, end + 1):  # Assuming integer range for simplicity
            result.extend(self.indices[column].get(value))
        return result

    def create_index(self, column_number):
        """
        # optional: Create index on specific column
        """
        if self.indices[column_number] is None:
            # Create a new dictionary to serve as the index for this column
            self.indices[column_number] = BPTreeIndex()

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

### B+ Tree Implementation
class BPTreeIndex:
    def __init__(self):
        self.tree = BPTree(n=5) # Adjust n as needed to test performance
    
    def get(self, val) -> list[RID]:
        leaf = self.tree.search_node(val)
        values = leaf.point_query_node(val)
        if values is None:
            return []
        else:
            # Maybe flatten?
            return values
        
    def get_range(self, begin, end):
        results = self.tree.range_query_tree(begin, end)
        # Will need to flatten results, becuase could have buckets in BPTree
        # i.e. multiple values per key
        flattened_results = [val for sublist in results for val in sublist]
        return flattened_results
    
    def insert(self, val, rid):
        self.tree.insert(val, rid)

    def delete(self, val):
        """
        !!! Values will be deleted from leafs, but tree won't rebalance yet.
        """
        self.tree.delete(val)


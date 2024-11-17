# %%
import math

from lstore import config

from lstore.index_types.bptree_node import BPTreeNode
from lstore.index_types.index_type import IndexType

from lstore.storage.rid import RID

class BPTree:
    def __init__(self, n):
        """
        Still figuring out the params
        n: the maximum number of keys in a node
        """

        self.n = n        
        self.root = BPTreeNode(n) # Initializes tree with a root node
        self.root.is_leaf = True # In the beginning, the root is a leaf

    def insert(self, key_insert, value_insert):
        """
        Inserts a key/value pair into the tree and splits if full
        key_insert: a single key to insert
        value_insert: a single value to insert
        """
        leaf_node = self.search_node(key_insert) # 1) Find right leaf node
        leaf_node.leaf_insert(leaf_node, key_insert, value_insert) # 2) Insert key/value pair
        if len(leaf_node.keys) > self.n - 1: # 3) If full, split the node
            self.split_node(leaf_node)

    def delete(self, key_delete):
        """
        TODO: Implement for persistent memory
        """
        leaf_node = self.search_node(key_delete)
        leaf_node.leaf_delete(leaf_node, key_delete)
        
        if len(leaf_node.keys) < math.ceil(self.n / 2):
            print('UGHHH doing rebalancing later')



    def split_node(self, node_to_split):
        """
        Splits a given node and updates the tree structure
        """
        
        new_node = BPTreeNode(self.n)
        new_node.is_leaf = node_to_split.is_leaf
        new_node.parent_node = node_to_split.parent_node 

        mid_index = len(node_to_split.keys) // 2 # Splits full node in half

        if node_to_split.is_leaf:
            # New node takes upper half
            new_node.keys = node_to_split.keys[mid_index:]
            new_node.values = node_to_split.values[mid_index:]
            # Old node takes the lower half. Facilitates forward pointer!!
            node_to_split.keys = node_to_split.keys[:mid_index]
            node_to_split.values = node_to_split.values[:mid_index]

            # New node (right of old node) takes old node's forward key
            new_node.forward_key = node_to_split.forward_key
            # Old node (left of new node) point to new node. 
            node_to_split.forward_key = new_node
            
            # Sepperator is the first key in the new node
            sepperator = new_node.keys[0]

        else: # Node to split is internal
            sepperator = node_to_split.keys[mid_index]

            # New node takes upper half with mid_index going to upstream parent
            new_node.keys = node_to_split.keys[mid_index + 1:]
            new_node.values = node_to_split.values[mid_index + 1:]

            # Old node takes lower half
            node_to_split.keys = node_to_split.keys[:mid_index] # keys up to mid_index (not included)
            node_to_split.values = node_to_split.values[:mid_index + 1] # need keys + 1 pointers for children

            # Update the child nodes' parent pointers for new node
            for child in new_node.values:
                child.parent_node = new_node

        # if node_to_split is the root make a new_root.
        if node_to_split.parent_node is None:
            new_root = BPTreeNode(self.n)
            new_root.is_leaf = False
            new_root.keys = [sepperator] # New root takes keys from old_node seperator
            new_root.values = [node_to_split, new_node] # New root points to old_node and new_node further down tree
            # Assign new parent to downstream nodes
            node_to_split.parent_node = new_root
            new_node.parent_node = new_root
            self.root = new_root
            if config.DEBUG_PRINT:
                pass
                #print(f'New root created on {new_root.keys}')

        else:
            self.insert_at_parent(node_to_split.parent_node, sepperator, new_node)


    def insert_at_parent(self, parent_node, key_insert, child_node):
        """
        Inserts a key and child node into the parent node
        """
        index = 0
        # search for key's place in parent node
        while index < len(parent_node.keys) and parent_node.keys[index] < key_insert:
            index += 1

        # Insert once place is found
        parent_node.keys.insert(index, key_insert)
        parent_node.values.insert(index + 1, child_node)
        child_node.parent_node = parent_node

        # Split if parent is full
        if len(parent_node.keys) > self.n - 1:
            self.split_node(parent_node)


    def search_node(self, key_search):
        """
        Traverses the tree from root-downward until a leaf node is found.
        Returns the leaf node
        """
        current_node = self.root  # Start at the root

        while not current_node.is_leaf:  # Keep going until leaf
            i = 0
            # Find the child to follow
            while i < len(current_node.keys) and key_search >= current_node.keys[i]:
                i += 1
            current_node = current_node.values[i]

        # Once at a leaf node, return it
        return current_node

    def get_node(self, key_search):
        """
        Checks for a key/value pair in a leaf node after traversing the tree with search_node()
        """

        potential_leaf = self.search_node(key_search)
        leaf_keys = potential_leaf.keys
        leaf_values = potential_leaf.values
        for i in range(len(leaf_keys)):
            if leaf_keys[i] == key_search:
                value_search = leaf_values[i]
                #print(f'Key:{key_search} and RID(s):{value_search} found in leaf node')
                return True
       #print(f'Key:{key_search} not found in leaf node')
        return False # If the key/value pair is not found
    
    # can take primary key, can also take values
    def get_range_val(self, val_low, val_high):
        """
        Performs range query on tree returns list of RIDS
        """
        results = []
        # Find the leaf node with key_low
        low_leaf = self.search_node(val_low)
        while low_leaf:
            leaf_key_max = low_leaf.keys[-1]
            # Range query on a leaf
            leaf_results, next_leaf_pointer = low_leaf.range_query_leaf(val_low, val_high)
            results.extend(leaf_results)

            if leaf_key_max >= val_high:
                break
        
            # Otherwise, move to the next leaf node
            low_leaf = next_leaf_pointer

        return results

### B+ Tree Implementation
class BPTreeIndex(IndexType):
    def __init__(self, n=100):
        self.n = n
        self.tree = BPTree(n=n) # Adjust n as needed to test performance
    
    def get(self, val) -> list[RID]:
        leaf = self.tree.search_node(val)
        values = leaf.point_query_node(val)
        if values is None:
            return []
        else:
            # Return the latest inserted value in a list
            return [values[-1]]
        
    def get_range_val(self, begin, end):
        """
        Gets list of RIDs with column value all between begin and end value
        """
        results = self.tree.get_range_val(begin, end)
        # Extract the latest value (most recent) from each key's values
        latest_values = [sublist[-1] for sublist in results if sublist]
        return latest_values
    
    def get_range_key(self, begin, end):
        """
        Gets list of RIDs with Prim ID all between begin and end value
        """
        return self.get_range_val(begin, end)
    
    def insert(self, val, rid):
        self.tree.insert(val, rid)

    def delete(self, val):
        """
        !!! Values will be deleted from leafs, but tree won't rebalance yet.
        """
        self.tree.delete(val)

    def clear(self):
        self.tree = BPTree(n=self.n)

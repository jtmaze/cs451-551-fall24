
#%%
# 
import math

#from lstore import config
#from lstore.index_types.bptree_node import BPTreeNode
#from lstore.index_types.index_type import IndexType

#from lstore.storage.rid import RID


class BPTreeNode:
    def __init__(self, n):
       
        """
        n: the maximum number of keys in a node
        """

        self.n = n 
        self.keys = [] # sorted list of keys
        self.values = [] # list of list of values (should be RIDs?)
        self.parent_node = None # Do I need a pointer to back to the parent node?
        self.forward_key = None # Pointer to the next leaf node
        self.is_leaf = False # Boolean denoting if the node is a leaf

    def leaf_insert(self, leaf, key_insert, value_insert):
        """
        Inserts a key/value pair into a leaf node
        leaf: Should checks if node is a leaf
        key_insert: a single key to insert
        value_insert: a single value (RID) to insert
        """
        current_keys = self.keys
        current_vals = self.values
        # 1) Leaf node is empty (easy case)
        if len(current_keys) == 0:
            self.keys.append(key_insert)
            self.values.append([value_insert])
            #print(f'Key:{key_insert} and RID:{value_insert} inserted on empty leaf node')

        # 2) Leaf node is not empty
        else:
            inserted = False
            for i in range(len(current_keys)):
                # First try to insert on lower keys and values
                if key_insert < current_keys[i]:
                    self.keys.insert(i, key_insert)
                    self.values.insert(i, [value_insert])
                    #print(f'Key:{key_insert} and RID:{value_insert} inserted on leaf node')
                    inserted = True
                    break
                
                elif key_insert == current_keys[i]: 
                    self.values[i].append(value_insert)
                    #print(f'Appening a new value {value_insert} to existing key {key_insert}')
                    inserted = True
                    break
                
            if not inserted:
                self.keys.append(key_insert)
                self.values.append([value_insert])
                #print(f'Key:{key_insert} and RID:{value_insert} inserted on leaf node')
                return
            
    def leaf_delete(self, leaf, key_delete, value_delete):

        """Deletes a specific value from the leaf node"""

        current_keys = self.keys
        current_values = self.values

        if self.is_leaf:
            for i in range(len(current_keys)):
                if key_delete == current_keys[i]:
                    try:
                        current_values[i].remove(value_delete)
                        print(f'Key {key_delete}, Value {value_delete} removed')
                        if current_keys[i] == 0:
                            del current_keys[i]
                            del current_values[i]
                            print(f'Key {key_delete} is empty, no more vals')
                        break
                    except ValueError:
                        print(f'Value {value_delete}, not found in leaf node')
                        break
        else:
            print(f'Key {key_delete} not found in leaf node')

 
        
    def point_query_node(self, search_key):
        """searches a leaf node for a single key's values"""
        
        current_vals = self.values
        current_keys = self.keys

        if self.is_leaf:
            for i in range(len(current_keys)):
                if current_keys[i] == search_key:
                    #print(f'Key:{search_key} found in leaf node')
                    return current_vals[i]
                
            if config.DEBUG_PRINT:
                pass
                #print(f'Key:{search_key} not found in leaf node')
                
            return None
        

    def range_query_leaf(self, key_low, key_high):
        """
        Inputs: the low and high key for a range query on a leaf 
        
        Returns:
        - A list of values within the keys range.
        - A next pointer IF needed 
        """
        current_keys = self.keys
        current_vals = self.values
        results = []
        next_pointer = None

        leaf_max_key = current_keys[-1]
        # 1) Easy case, entire range query on leaf node
        if key_high <= leaf_max_key:
            next_pointer = None
        # 2) Harder case follow pointer right to higher node    
        else:
            next_pointer = self.forward_key

        for i in range(len(current_keys)):
            if current_keys[i] > key_high:
                break # More efficient to end loop when done
            if key_low <= current_keys[i]:
                results.append(current_vals[i])

        return(results, next_pointer)

# %%

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

    def delete(self, key_delete, value_delete):
        """
        TODO: Implement for persistent memory
        """
        leaf_node = self.search_node(key_delete)
        leaf_node.leaf_delete(leaf_node, key_delete, value_delete)
        
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
            #if config.DEBUG_PRINT:
                #pass
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
    
    def scan_all_leafs(self):
        """
        Starts at left-most leaf and scans all leaf nodes using forward pointers
        Returns: Linked list of all keys and values in leaf node
        """
        results = []

        # 1. Find the left-most (lowest) leaf node
        current_node = self.root
        while not current_node.is_leaf:
            if not current_node.values:
                break # Error handling for empty tree
            current_node = current_node.values[0] # Always take the first child

        # 2. Traverse leaf nodes using forward pointers
        while current_node:
            for key, vals in zip(current_node.keys, current_node.values):
                for val in vals:
                    results.append((key, val))
            current_node = current_node.forward_key

        return results
    
    # Helpers ---------------------


    def display(self, node=None, level=0):
        """
        For testing BPTree Methods
        Couldn't actually hide method as true helper
        """
        if node is None:
            node = self.root
        indent = '    ' * level
        if node.is_leaf:
            print(f"{indent}Level {level} Leaf Node: Keys={node.keys}, Values={node.values}")
        else:
            print(f"{indent}Level {level} Internal Node: Keys={node.keys}")
            for child in node.values:
                self.display(child, level + 1)

# %%

bpt = BPTree(n=4)

data = [
    (7, 'RID05'),
    (8, 'RID03'),
    (10, 'RID78'),
    (10, 'RID09'),
    (10, 'RID11'),
    (11, 'RID90'),
    (12, 'RID07'),
    (13, 'RID12'),
    (15, 'RID14'),
    (16, 'RID60')
]

for grade, rid in data:
    bpt.insert(grade, rid)


# %%

#bpt.display()
scan_results = bpt.scan_all_leafs()

print(scan_results)

# %%

delete_data = [
    (10, 'RID09'), 
    (12, 'RID07'),
    (10, 'RID78')
]

for grade, rid in delete_data:
    bpt.delete(grade, rid)

bpt.display()

# %%
scan_results = bpt.scan_all_leafs()

print(scan_results)

# %%

for grade, rid in delete_data:
    bpt.delete(grade, rid)

bpt.display()

# %%

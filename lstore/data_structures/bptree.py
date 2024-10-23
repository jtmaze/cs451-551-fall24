# %%
import math
import pprint as pp # For debugging, !!! remove later

# %%
class BPTreeNode:
    def __init__(self, n):
       
        """
        n: the maximum number of keys in a node
        """

        self.n = n 
        self.keys = [] # sorted list of keys
        self.values = [] # list of values (should be RIDs?)
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
        current_vals = self.values
        current_keys = self.keys
        # 1) Leaf node is empty (easy case)
        if len(current_vals) == 0 and len(current_keys) == 0:
            self.values.append(value_insert)
            self.keys.append(key_insert)
            print(f'Key:{key_insert} and RID:{value_insert} inserted on empty leaf node')

        # 2) Leaf node is not empty
        else:
            for i in range(len(current_vals)):
                # First try to insert on lower keys and values
                if key_insert < current_keys[i]:
                    self.values.insert(i, value_insert)
                    self.keys.insert(i, key_insert)
                    print(f'Key:{key_insert} and RID:{value_insert} inserted on leaf node')
                    return
                # Then, append on the end of keys and values are greater than the current max
                elif key_insert > current_keys[i]: 
                    self.values.append(value_insert)
                    self.keys.append(key_insert)
                    print(f'Key:{key_insert} and RID:{value_insert} inserted on leaf node')
                    return
                # Throw an error if the key or RID is already in the node
                elif key_insert == current_keys[i]: # Not sure if I shold also check current keys??
                    self.values.append(value_insert)
                    self.keys.append([key_insert])
                    return
                # Catch all for unhandled cases (e.g. key_insert < current_keys BUT value_insert > current_vals)
                else:
                    print('ERROR: Unhandled case for leaf_insert')
                    return

    def point_query_node(self, key):
        current_node = self
        current_vals = self.values
        current_keys = self.keys

        if key in current_keys:
            index = current_keys.index(key)
            return current_vals[index]
        
        else:
            print(f'Key:{key} not found in leaf nodes')

        pass
    
    def range_query_node(self, low_val, high_val):
        current_node = self
        current_vals = self.values
        current_keys = self.keys

        # 1) Easy case if the range is within the current node
        #if key_high < current_node.keys[-1]:
            #return current_node.values[key_low:key_high] # this is probably wrong
        
        # 2) Harder case return current nodes values and point to next node
        #else:
        pass



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
        Inserts a key/value pair into the tree
        key_insert: a single key to insert
        value_insert: a single value (RID) to insert
        """
        leaf_node = self.search_node(key_insert) 
        leaf_node.leaf_insert(leaf_node, key_insert, value_insert) 
        if len(leaf_node.keys) > self.n -1: # If the leaf node is full, split the node
            self.split_node(leaf_node)


    def split_node(self, node_to_split):
        """
        Splits a given node and updates the tree structure
        """
        
        new_node = BPTreeNode(self.n)
        new_node.is_leaf = node_to_split.is_leaf
        new_node.parent_node = node_to_split.parent_node 

        split_index = len(node_to_split.keys) // 2 #

        new_node.keys = node_to_split.keys[split_index:]
        new_node.values = node_to_split.values[split_index:]
        node_to_split.keys = node_to_split.keys[:split_index]
        node_to_split.values = node_to_split.values[:split_index]

        if node_to_split.is_leaf:
            new_node.forward_key = node_to_split.forward_key
            node_to_split.forward_key = new_node

        else:
            for child in new_node.values:
                child.parent_node = new_node

        if node_to_split.parent_node is None:
            new_root = BPTreeNode(self.n)
            new_root.keys = [new_node.keys[0]]
            new_root.values = [node_to_split, new_node]
            new_root.is_leaf = False
            node_to_split
            new_node.parent_node = new_root
            self.root = new_root
        else:
            self.insert_at_parent(node_to_split.parent_node, new_node.keys[0], new_node)

    def insert_at_parent(self, parent_node, key_insert, child_node):
        """
        Inserts a key and child node into the parent node
        """
        index = 0
        while index < len(parent_node.keys) and parent_node.keys[index] < key_insert:
            index += 1

        parent_node.keys.insert(index, key_insert)
        parent_node.values.insert(index + 1, child_node)

        child_node.parent_node = parent_node

        if len(parent_node.keys) > self.n - 1:
            self.split_node(parent_node)


    def search_node(self, key_search):
        """
        Traverses the tree from root-downward until a leaf node is found.
        key_search: key to search for
        """
        current_node = self.root  # Start at the root

        while not current_node.is_leaf:  # Keep going until we reach a leaf
            i = 0
            # Find the child to follow
            while i < len(current_node.keys) and key_search >= current_node.keys[i]:
                i += 1
            current_node = current_node.values[i]

        # Once we're at a leaf node, return it
        return current_node

    def get_node(self, key_search, value_search):
        """
        Checks for a key/value pair in a leaf node after traversing the tree with search_node()
        """

        potential_leaf = self.search_node(value_search)
        leaf_keys = potential_leaf.keys
        leaf_values = potential_leaf.values
        for i in range(len(leaf_values)):
            if leaf_values[i] == value_search and leaf_keys[i] == key_search:
                print(f'Key:{key_search} and RID:{value_search} found in leaf node')
                return True
            else:
                return False
        return False # If the key/value pair is not found



# %% Testing down here because I'm a noob

order = 4  
bptree = BPTree(order)

# Insert key/value pairs
keys_to_insert = [10, 20, 5, 6, 12, 30, 7, 17, 50, 53, 8, 8]
values_to_insert = [100, 200, 50, 60, 120, 300, 70, 170, 500, 520, 8, 720]

for key, value in zip(keys_to_insert, values_to_insert):
    bptree.insert(key, value)

# Function to print the tree structure
def print_tree(node, level=0):
    indent = '   ' * level
    if node.is_leaf:
        print(f"{indent}Leaf Node: Keys={node.keys} and Leaf Values={node.values}")
    else:
        print(f"{indent}Internal Node: Keys={node.keys}")
        for child in node.values:
            print_tree(child, level + 1)

print_tree(bptree.root)



# %%

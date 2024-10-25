# %%
import math
import pprint as pp # For debugging, !!! remove later

from data_structures.bptree_node import BPTreeNode

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
            print(f'New root created on {new_root.keys}')

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
                print(f'Key:{key_search} and RID(s):{value_search} found in leaf node')
                return True
        print(f'Key:{key_search} not found in leaf node')
        return False # If the key/value pair is not found
    
    def range_query_tree(self, key_low, key_high):
        """
        Performs range query on tree returns list of values
        """
        results = []
        # Find the leaf node with key_low
        low_leaf = self.search_node(key_low)

        while low_leaf:
            leaf_key_max = low_leaf.keys[-1]
            # Range query on a leaf
            leaf_results, next_leaf_pointer = low_leaf.range_query_leaf(key_low, key_high)
            results.extend(leaf_results)

            if leaf_key_max >= key_high:
                break
        
            # Otherwise, move to the next leaf node
            low_leaf = next_leaf_pointer

        return results


# %% Testing down here because I'm a noob
# Function to print the tree structure
def print_tree(node, level=0):
    indent = '   ' * level
    if node.is_leaf:
        print(f"{indent}Leaf Node: Keys={node.keys} and Leaf Values={node.values}")
    else:
        print(f"{indent}Internal Node: Keys={node.keys}")
        for child in node.values:
            print_tree(child, level + 1)


order = 4  
bptree = BPTree(order)

# Insert key/value pairs
keys_to_insert = [10, 20, 5, 6, 12, 30, 7, 17, 50, 53, 8, 8]
values_to_insert = [100, 200, 50, 60, 120, 300, 70, 170, 500, 520, 1776, 720]

for key, value in zip(keys_to_insert, values_to_insert):
    bptree.insert(key, value)

print_tree(bptree.root)

bptree.delete(key_delete=5)

# %%
search_key = 8
test_leaf = bptree.search_node(search_key)
print(test_leaf.values)
r = test_leaf.point_query_node(search_key)
print(r)

# r = test_leaf.point_query_node(search_key)
# print(r)
test_range = bptree.range_query_tree(7, 12)

del bptree

# %%
import random 

def make_unique_rando(lenght, min, max):
    rando = []
    while len(rando) < lenght:
        n = random.randint(min, max)
        if n not in rando:
            rando.append(n)
    return rando

def make_nonunique_rando(lenght, min, max):
    rando = []
    while len(rando) < lenght:
        n = random.randint(min, max)
        rando.append(n)
    return rando

order = 10
bptree = BPTree(order)
keys_to_insert = make_nonunique_rando(1000, 1, 20)
values_to_insert = make_unique_rando(1000, 1000, 100000)

for k, v in zip(keys_to_insert, values_to_insert):
    bptree.insert(k, v)

print_tree(bptree.root)

search_key = 8
test_results = bptree.search_node(search_key).point_query_node(search_key)
print(test_results)

# %%

bptree.delete(key_delete=3)
# %%
search_key = 3
test_results = bptree.search_node(search_key).point_query_node(search_key)
print(test_results)
# %%

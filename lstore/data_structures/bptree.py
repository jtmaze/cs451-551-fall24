import math
import pprint as pp # For debugging, !!! remove later

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
                if value_insert < current_vals[i] and key_insert < current_keys[i]:
                    self.values.insert(i, value_insert)
                    self.keys.insert(i, key_insert)
                    print(f'Key:{key_insert} and RID:{value_insert} inserted on leaf node')
                    return
                # Then, append on the end of keys and values are greater than the current max
                elif value_insert > current_vals[i] and key_insert > current_keys[i]: 
                    self.values.append(value_insert)
                    self.keys.append(key_insert)
                    print(f'Key:{key_insert} and RID:{value_insert} inserted on leaf node')
                    return
                # Throw an error if the key or RID is already in the node
                elif value_insert == current_vals[i] or key_insert == current_keys[i]: # Not sure if I shold also check current keys??
                    print(f'ERROR: Duplicate RID {value_insert} or key {key_insert} Violates L-Store unique RID Constraint')
                    return
                # Catch all for unhandled cases (e.g. key_insert < current_keys BUT value_insert > current_vals)
                else:
                    print('ERROR: Unhandled case for leaf_insert')
                    return

    def point_query_node(self, key):
        current_node = self
        current_vals = self.values
        current_keys = self.keys

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
            self.split_node(key_insert, value_insert)


    def split_node(self, key_insert, value_insert):
        """
        Splits a given node and updates the tree structure
        """
        
        # Need to figure out some way to check if node is a leaf or internal???
        previous_node = self.search_node(value_insert) # Searches for appropriate leaf node
        previous_node.leaf_insert(previous_node, key_insert, value_insert) # First try insert at leaf

        # If leaf is full, split the node
        if (len(previous_node.keys) == previous_node.n):
            print('Node is full, splitting')
            # The new node will hold 1/2 the values of the previous node
            new_node = BPTreeNode(previous_node.n)
            new_node.is_leaf = True
            new_node.parent_node = previous_node.parent_node # Seems like previous_node.parent_node is not recognized above
            pp.pp(previous_node.parent_node, new_node.parent_node)
            # Split the two nodes in half
            split_size = int(math.ceil(self.n / 2)) - 1
            # Populate the new node with the second half of the previous node
            new_node.keys = previous_node.keys[split_size + 1:]
            new_node.values = previous_node.values[split_size + 1:]
            # Keep the first half of the previous node in the previous node
            previous_node.keys = previous_node.keys[:split_size + 1]
            previous_node.values = previous_node.values[:split_size + 1]
            # Point the previous node to the new node
            previous_node.forward_key = new_node
            self.insert_above(previous_node, new_node.values[0], new_node) # Put the smallest value of new_node in the parent node

            pp.pp(f""" NEW NODE: {new_node}
                  ------------------------------------------------
                  SPLIT NODE: {previous_node} """)


    def search_node(self, key_search):
        """
        Traverses the tree from root-downward until a leaf node is found. 
        key_search: key to search for
        value_search: value to search for
        """
        current_node = self.root # Start at the root
        while (current_node.is_leaf == False): # Keeping going until we reach a leaf
            node_keys = current_node.values 
            for i in range(len(node_keys)):
                if (key_search == node_keys[i]): # The search equals the current
                    result_node = current_node.keys[i + 1] # The result node points to the right
                    break
                elif (key_search < node_keys[i]): # Search is less than the current
                    result_node = current_node.keys[i] # The result node points to the left
                    break
                elif (i + 1 == len(node_keys)): # Search greater than all node vals
                    result_node = current_node.keys[i + 1] # points to the right
                    break
                else:
                    print("search_node: Error")

        return result_node

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




"""
    def point_query(self, key):

        value = 'somestuff'

        return value

    def range_query(self, key_low, key_high):
        
        # Is it appropriate to return a list???
        values = ['some other stuff']

        return values

"""

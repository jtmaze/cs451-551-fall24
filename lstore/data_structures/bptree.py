import math

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
        self.is_leaf = False

        def leaf_insert(self, leaf, key_insert, value_insert):
            """
            leaf: Should checks if node is a leaf
            key_insert: a single key to insert
            value_insert: a single value (RID) to insert
            ?? Should be no reseason to accept lists of keys and values ??
            """
            current_vals = self.values
            current_keys = self.keys
            # 1) Leaf node is empty (easy case)
            if len(current_vals) == 0:
                self.values.append(value_insert)
                self.keys.append(key_insert)
                print(f'Key:{key_insert} and RID:{value_insert} inserted on empty leaf node')

            # 2) Leaf node is not empty
            else:
                for i in range(len(current_vals)):
                    # Throw error if RID duplicated
                    if value_insert == current_vals[i] or key_insert == current_keys[i]: # Not sure if I shold also check current keys??
                        print(f'ERROR: Duplicate RID {value_insert} or key {key_insert} Violates L-Store unique RID Constraint')
                        return
                    # Append on the end of keys and values are greater than the current max
                    elif value_insert > current_vals[i] and key_insert > current_keys[i]: 
                        self.values.append(value_insert)
                        self.keys.append(key_insert)
                        print(f'Key:{key_insert} and RID:{value_insert} inserted on leaf node')
                        return
                    # Insert keys and values into correct position
                    elif value_insert < current_vals[i] and key_insert < current_keys[i]:
                        self.values.insert(i, value_insert)
                        self.keys.insert(i, key_insert)
                        print(f'Key:{key_insert} and RID:{value_insert} inserted on leaf node')
                        return
                    # Catch-all error
                    else:
                        print('ERROR: Unhandled case for leaf_insert')
                        return

        def point_query_n(self, key):
            current_node = self
            current_vals = self.values
            current_keys = self.keys

            pass
        
        def range_query_n(self, low_val, high_val):
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
    
    def insert_node(self, key_insert, value_insert):
        
        # Need to figure out some way to check if node is a leaf or internal...
        previous_node = None # Need to build pointer to previous node
        previous_node.leaf_insert(previous_node, key_insert, value_insert) # First try insert at leaf

        if (len(previous_node.keys) > self.n):
            print('Node is full, splitting')
            pass

        print('Key(s) inserted')
        pass

    def search_node(self, key, value):

        print('Key(s) deleted')
        pass



    def point_query(self, key):

        value = 'somestuff'

        return value

    def range_query(self, key_low, key_high):
        
        # Is it appropriate to return a list???
        values = ['some other stuff']

        return values


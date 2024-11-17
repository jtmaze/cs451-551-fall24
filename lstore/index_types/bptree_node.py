import math

from lstore import config

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
            
    def leaf_delete(self, leaf, key_delete):

        """Deletes a specific key and its valuesfrom a leaf node"""
        
        current_keys = self.keys
        current_values = self.values

        if self.is_leaf:
            for i in range(len(current_keys)):
                if key_delete == current_keys[i]:
                    del current_keys[i]
                    del current_values[i]
                    #print(f'Key: {key_delete} deleted')
                    return
                
            if config.DEBUG_PRINT:
                pass
                #print(f'Key {key_delete} not found')
        

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


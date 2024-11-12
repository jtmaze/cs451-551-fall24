from lstore.index_types.index_type import IndexType

from lstore.storage.rid import RID

class DictIndex(IndexType):
    def __init__(self):
        self.data = dict()

    def get(self, val) -> list[RID]:
        output = self.data.get(val, None)

        if output is None:
            return []
        
        return [output]

    def get_range_key(self, begin, end) -> list[RID]:
        """
            Takes in a begin key and end key
            Returns list of RIDs of all keys inbetween
        """ 
        output = []
        for val in range(begin, end + 1):
            output.append(self.data.get(val, None))
        
        return output
    
    def get_range_val(self, begin, end) -> list[RID]:
        """
            Takes in begin val and end val
            returns list of all RIDS associated with the value
        """
        results = []
        for i in self.data:
            if self.data[i] >= begin and self.data[i] <= end:
                results.extend(i)

        return results

    def insert(self, key, val):
        # Val is RID if primary key,
        # Val is value if not primary key
        self.data[key] = val

    def delete(self, val):
        del self.data[val]

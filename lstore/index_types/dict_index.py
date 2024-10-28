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

    def get_range(self, begin, end):
        raise NotImplementedError()

    def insert(self, val, rid):
        self.data[val] = rid

    def delete(self, val):
        del self.data[val]

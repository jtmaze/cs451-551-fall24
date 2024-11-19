
from typing import Type

from lstore.index_types.index_type import IndexType

from lstore.index_types.bptree import BPTreeIndex

from lstore.index_types.dict_index import DictIndex

class IndexConfig:
    """
    index_type: Two options, for now: 1) BPTreeIndex 2) DictIndex (hash)
    node_size: only applies to the BPTreeIndex, number of items in leaf
    """
    def __init__(
        self, 
        index_type: Type[IndexType] = BPTreeIndex,
        node_size: int = 10,
        index_columns: list[int] = None
    ) -> None:
        self.index_type = index_type
        self.node_size = node_size
        self.index_cols = index_columns
        
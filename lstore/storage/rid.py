"""
RID (record identifier) definition.

Should encode the physical location of records on the disk while being
hashable for fast buffer access.
"""

from typing import Literal

import uuid

_RID_BITS = (
    1,   # is_base
    1,   # tombstone
    64,  # UUID
)

# Bit shift needed to get to field (ie cumulative field offset)
_RID_SHIFTS = tuple(
    sum(_RID_BITS[:i]) for i in range(len(_RID_BITS))
)

# Mask for field (ie 1s for every bit in field. ex 48 1s for 48 bit field)
_RID_MASKS = tuple(
    (1 << bits) - 1 for bits in _RID_BITS
)

# Bitmasks for individial fields (ie 1s for field and 0s elsewhere)
_FIELD_MASKS = tuple(
    _RID_MASKS[i] << _RID_SHIFTS[i] for i in range(len(_RID_BITS))
)

class RID:

    def __init__(self, rid_int: int):
        self._rid = rid_int

    @classmethod
    def from_params(cls, is_base: Literal[0, 1], tombstone: Literal[0, 1]):
        """
        Constructor with parameters. ex rid = RID.from_params(...)
        """
        # Ensure correct amount of bits
        is_base &= _RID_MASKS[0]
        tombstone &= _RID_MASKS[1]
        id = uuid.uuid4().int & _RID_MASKS[2]

        # Shift and combine fields into integer
        rid_int = 0
        for i, field in enumerate((is_base, tombstone, id)):
            rid_int |= (field << _RID_SHIFTS[i])

        # Create object and return
        return cls(rid_int)
    
    @property
    def rid(self):
        return self._rid
    
    @property
    def is_base(self):
        return (self.rid & _FIELD_MASKS[0]) >> _RID_SHIFTS[0]

    @property
    def tombstone(self):
        return (self.rid & _FIELD_MASKS[1]) >> _RID_SHIFTS[1]
    
    @property
    def uid(self):
        return (self.rid & _FIELD_MASKS[2]) >> _RID_SHIFTS[2]
    
    def to_bytes(self, length=8, byteorder="big"):
        return self.rid.to_bytes(length, byteorder)
    
    def __hash__(self) -> int:
        return hash(self.rid)
    
    def __eq__(self, rhs) -> bool:
        # Used by dict
        return self.rid == rhs.rid
    
    @classmethod
    def get_dead_record(cls):
        return cls(0)
    
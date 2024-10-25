"""
RID (record identifier) definition.

Should encode the physical location of records on the disk while being
hashable for fast buffer access.
"""

from typing import Literal

import time

_RID_BITS = (
    1,   # tombstone
    48,  # timestamp
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
        self.rid = rid_int

    @classmethod
    def from_params(cls, tombstone: Literal[0, 1]):
        """
        Constructor with parameters. ex rid = RID.from_params(...)
        """
        # Ensure correct amount of bits
        tombstone &= _RID_MASKS[0]
        ts = int(time.time() * 1000) & _RID_MASKS[1]

        # Shift and combine fields into integer
        rid_int = 0
        for i, field in enumerate(tombstone, ts):
            rid_int |= (field << _RID_SHIFTS[i])

        # Create object and return
        return cls(rid_int)

    @property
    def tombstone(self):
        return (self.rid & _FIELD_MASKS[0]) >> _RID_SHIFTS[0]
    
    @property
    def timestamp(self):
        return (self.rid & _FIELD_MASKS[1]) >> _RID_SHIFTS[1]
    
    def to_bytes(self):
        return self.rid.to_bytes()
    
    def __hash__(self) -> int:
        return hash(self.rid)
    
    @classmethod
    def get_dead_record(cls):
        return cls(0)
    
"""
RID (record identifier) definition.

Should encode the physical location of records on the disk while being
hashable for fast buffer access.
"""

from typing import Literal

from enum import IntEnum

# Setup -----------------------------------------

class _RIDField(IntEnum):
    """RID attribute index"""
    ID_NUM = 0
    IS_BASE = 1
    TOMBSTONE = 2


_RID_BITS = (
    48,  # ID
    1,   # is_base
    1,   # tombstone
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


# Class -----------------------------------------

class RID:
    ctr = 2 ** _RID_BITS[_RIDField.ID_NUM]

    def __init__(self, rid_int: int):
        self._rid = rid_int

    @classmethod
    def from_params(cls, is_base: Literal[0, 1], tombstone: Literal[0, 1]):
        """
        Constructor with parameters. ex rid = RID.from_params(...)
        """
        # Set ID and ensure correct amount of bits
        id = RID.ctr & _RID_MASKS[_RIDField.ID_NUM]
        RID.ctr -= 1

        # Ensure correct amount of bits
        is_base &= _RID_MASKS[_RIDField.IS_BASE]
        tombstone &= _RID_MASKS[_RIDField.TOMBSTONE]

        # Shift and combine fields into integer
        rid_int = 0
        for i, field in enumerate((id, is_base, tombstone)):
            rid_int |= (field << _RID_SHIFTS[i])

        # Create object and return
        return cls(rid_int)

    @property
    def rid(self):
        return self._rid
    
    def __int__(self):
        return self._rid

    @property
    def uid(self):
        return self._get_field(_RIDField.ID_NUM)
    
    @property
    def is_base(self):
        return self._get_field(_RIDField.IS_BASE)

    @property
    def tombstone(self):
        return self._get_field(_RIDField.TOMBSTONE)

    def to_bytes(self, length=8, byteorder="big", signed=True):
        # TODO: Ensure signed=False works without getting in way of negative data ints
        # TODO: Test larger lengths when fields are added
        return self.rid.to_bytes(length, byteorder, signed=signed)

    def __hash__(self) -> int:
        return hash(self.rid)

    def __eq__(self, rhs) -> bool:
        # Used by dict
        if type(rhs) == RID:
            return self.rid == rhs.rid
        else:
            return self.rid == rhs
    
    def __gt__(self, rhs) -> bool:
        if type(rhs) == RID:
            return self.rid > rhs.rid
        else:
            return self.rid > rhs
    
    def __ls__(self, rhs) -> bool:
        if type(rhs) == RID:
            return self.rid < rhs.rid
        else:
            return self.rid < rhs
    
    def __ge__(self, rhs) -> bool:
        if type(rhs) == RID:
            return self.rid >= rhs.rid
        else:
            return self.rid >= rhs
    
    def __le__(self, rhs) -> bool:
        if type(rhs) == RID:
            return self.rid <= rhs.rid
        else:
            return self.rid <= rhs

    def _get_field(self, idx):
        return (self.rid & _FIELD_MASKS[idx]) >> _RID_SHIFTS[idx]


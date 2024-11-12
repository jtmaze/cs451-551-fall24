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
    UID = 0
    PAGES_ID = 1
    PAGES_OFFSET = 2
    IS_BASE = 3
    TOMBSTONE = 4

_TOTAL_RID_BITS = 128
_TOTAL_RID_BYTES = _TOTAL_RID_BITS // 8

_RID_BITS = (
    48,  # UID
    20,  # pages_id
    12,  # offset
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
    ctr = 2 ** _RID_BITS[_RIDField.UID] - 1

    def __init__(self, rid_int: int):
        self._rid = rid_int

    @classmethod
    def from_params(
        cls,
        is_base: Literal[0, 1],
        tombstone: Literal[0, 1],
        pages_id: int,
        pages_offset: int,
    ):
        """
        Constructor with parameters. ex rid = RID.from_params(...)
        """
        # Set ID and ensure correct amount of bits
        uid = RID.ctr & _RID_MASKS[_RIDField.UID]
        RID.ctr -= 1

        # Ensure correct amount of bits
        pages_id &= _RID_MASKS[_RIDField.PAGES_ID]
        pages_offset &= _RID_MASKS[_RIDField.PAGES_OFFSET]
        is_base &= _RID_MASKS[_RIDField.IS_BASE]
        tombstone &= _RID_MASKS[_RIDField.TOMBSTONE]

        # Shift and combine fields into integer
        rid_int = 0
        for i, field in enumerate((uid, pages_id, pages_offset, is_base, tombstone)):
            rid_int |= (field << _RID_SHIFTS[i])

        # Create object and return
        return cls(rid_int)

    @property
    def rid(self):
        # Gets actual integer, int(rid) is also supported
        return self._rid

    @property
    def uid(self):
        return self._get_field(_RIDField.UID)
    
    @property
    def pages_id(self):
        return self._get_field(_RIDField.PAGES_ID)
    
    @property
    def pages_offset(self):
        return self._get_field(_RIDField.PAGES_OFFSET)
    
    @property
    def is_base(self):
        return self._get_field(_RIDField.IS_BASE)

    @property
    def tombstone(self):
        return self._get_field(_RIDField.TOMBSTONE)

    def to_bytes(self, length=_TOTAL_RID_BYTES, byteorder="big", signed=True):
        # TODO: Ensure signed=False works without getting in way of negative data ints
        # TODO: Test larger lengths when fields are added
        return self.rid.to_bytes(length, byteorder, signed=signed)
    
    def __int__(self):
        return self._rid

    def __hash__(self) -> int:
        return hash(self.rid)

    def __eq__(self, rhs) -> bool:
        # Used by dict
        return self._rid == int(rhs)
    
    def __gt__(self, rhs) -> bool:
        return self._rid > int(rhs)
    
    def __lt__(self, rhs) -> bool:
        return self._rid < int(rhs)
    
    def __ge__(self, rhs) -> bool:
        return self._rid >= int(rhs)
    
    def __le__(self, rhs) -> bool:
        return self._rid <= int(rhs)
        
    # Helpers -------------------

    def _get_field(self, idx):
        return (self.rid & _FIELD_MASKS[idx]) >> _RID_SHIFTS[idx]


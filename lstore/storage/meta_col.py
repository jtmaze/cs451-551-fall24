from enum import IntEnum

class MetaCol(IntEnum):
    INDIR = 0      # Base: RID of latest tail; Tail: RID of prev
    RID = 1        # Record ID (and index/location/hashable in page directory)
    SCHEMA = 2     # Bits representing cols, 1s where updated
    
    # TIME = 3       # Timestamp for both base and tail record

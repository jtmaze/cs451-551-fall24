from enum import Enum

class MetaCol(Enum):
    INDIR = 0      # Base: RID of latest tail; Tail: RID of prev
    RID = 1        # Record ID (and index/location/hashable in page directory)
    TIMESTAMP = 2  # Timestamp for both base and tail record
    SCHEMA = 3     # Bits representing cols, 1s where updated

    COL_COUNT = 4  # Number of metadata columns

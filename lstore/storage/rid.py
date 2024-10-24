"""
RID (record identifier) definition.

Should encode the physical location of records on the disk while being
hashable for fast buffer access.
"""

from typing import Generator

from collections import namedtuple
import time

"""
Create an RID namedtuple.

This offers the benefits of classes with less overhead. Importantly, named
tuples are hashable and can be used to clearly demarcate location in
physical memory.
"""
RID = namedtuple("RID", ["timestamp", "tombstone", "volume_id"])

DEAD_RECORD_RID = RID(0.0, True, 0)


def rid_generator() -> Generator[RID, None, None]:
    """
    Generates an RID;

    TODO: Currently DOES NOT reflect physical location and will NOT be unique
    following closing of database; ie should be made to support persistence.
    """
    volume_id = 0

    while True:
        yield RID(time.time(), False, volume_id)

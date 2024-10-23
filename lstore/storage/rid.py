"""
RID (record identifier) definition.

Should encode the physical location of records on the disk while being
hashable for fast buffer access.
"""

from typing import Generator

from collections import namedtuple
import time

"""
Create an RID 'class' (not instance) as a named tuple.

This offers the benefits of classes with less overhead. Importantly, named
tuples are hashable and can be used.
"""
RID = namedtuple("RID", ["volume_id", "page_id", "offset", "timestamp"])


def rid_generator() -> Generator[RID, None, None]:
    """
    Generates an RID;
    
    TODO: Currently DOES NOT reflect physical location and will NOT be unique
    following closing of database; ie should be made to support persistence.
    """
    volume_id = 0
    page_id = 0
    offset = 0

    while True:
        yield RID(volume_id, page_id, offset, time.time())
        page_id += 1
        offset += 1

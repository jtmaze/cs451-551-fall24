"""
RID (record identifier) definition.

Should encode the physical location of records on the disk while being
hashable for fast buffer access.
"""

from collections import namedtuple

"""
Create an RID 'class' (not instance) as a named tuple.

This offers the benefits of classes with less overhead. Importantly, named
tuples are hashable and can be used.
"""
RID = namedtuple("RID", ["volume_id", "page_id", "offset"])


def rid_generator():
    """
    Generates an RID; currently DOES NOT reflect physical location, but
    should be made for persistance later.
    """
    # TODO: Coordinate with other modules for meaningful RIDs
    volume_id = 0
    page_id = 0
    offset = 0

    while True:
        yield RID(volume_id, page_id, offset)
        page_id += 1
        offset += 1

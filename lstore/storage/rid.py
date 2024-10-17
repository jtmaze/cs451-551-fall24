"""RID definition."""

from collections import namedtuple

"""
Create an RID 'class' (not instance) as a named tuple.

This offers the benefits of classes with less overhead. Importantly, named
tuples are hashable and can be used.

  :param page_id:
  :param slot: position of record within the page
"""
RID = namedtuple("RID", ["page_id", "slot"])

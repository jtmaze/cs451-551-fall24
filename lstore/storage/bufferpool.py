from collections import OrderedDict

class Bufferpool:
    """
    A simple bufferpool that uses a hash table to store pages in memory,
    using RIDs (Record IDs) as keys.
    """
    def __init__(self, max_size=None):
        # Max buffer size (optional), could be useful for future use in eviction policy
        self.max_size = max_size
        self.pages = OrderedDict()

    def get_page(self, rid):
        """
        Retrieves a page using the given RID from the bufferpool.
        """
        return self.pages.get(rid, None)

    def add_page(self, rid, page):
        """
        Adds a page to the bufferpool and associates it with the given RID.
        """
        # If page already in buffer, move to end of queue
        if rid in self.pages:
            self.pages.move_to_end(rid)

        self.pages[rid] = page

        # Remove oldest if buffer full (first in ordered dict)
        if len(self.pages) > self.max_size:
            self.pages.popitem(last=False)

    def remove_page(self, rid):
        """
        Removes a page from the bufferpool.
        """
        if rid in self.pages:
            del self.pages[rid]

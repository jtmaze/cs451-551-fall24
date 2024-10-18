class Bufferpool:
    """
    A simple bufferpool that uses a hash table to store pages in memory,
    using RIDs (Record IDs) as keys.
    """
    def __init__(self, max_size=None):
        # Max buffer size (optional), could be useful for future use in eviction policy
        self.max_size = max_size
        self.page_directory = {}

    def get_page(self, rid):
        """
        Retrieves a page using the given RID from the bufferpool.
        """
        return self.page_directory.get(rid, None)

    def add_page(self, rid, page):
        """
        Adds a page to the bufferpool and associates it with the given RID.
        """
        self.page_directory[rid] = page

    def remove_page(self, rid):
        """
        Removes a page from the bufferpool.
        """
        if rid in self.page_directory:
            del self.page_directory[rid]

from collections import OrderedDict

from lstore.page import Page

class PageTable:
    def __init__(self, tcols) -> None:
        self.ptable = OrderedDict()
        
        self.tcols = tcols
        self.size = 0

    def __getattr__(self, attr):
        return getattr(self.ptable, attr)

    def create_pages(self, pages_id):
        self.ptable[pages_id] = [Page(pages_id) for _ in range(self.tcols)]

        self.size += self.tcols

    def get_page(self, pages_id, col):
        try:
            return self.ptable[pages_id][col]
        except KeyError:
            return None

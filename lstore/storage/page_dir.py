from storage.bufferpool import Bufferpool
from storage.disk import Disk

from page import Page

class PageDirectory:
    """
    """

    def __init__(self, num_columns: int, buffer_size: int) -> None:
        self.num_columns = num_columns

        self.bufferpool = Bufferpool(buffer_size)

        self.disk = Disk()  # TODO: Support persistent memory

    def get_page(self, rid, cols=None) -> Page | None:
        """
        """
        # Check buffer
        page = self.bufferpool.get_page(rid)

        # Go to persistent memory if not in buffer
        if page is None:
            page = self.disk.get_page(rid)
            if page:
                self.bufferpool.add_page(rid, page)
        
        return page

    def add_page(self, rid, page):
        self.disk.add_page(rid, page)

        self.bufferpool.add_page(rid, page)

import os

from lstore import config

from lstore.page import Page

class Disk:
    PAGE_SIZE = config.PAGE_SIZE  # 4KB page size

    def __init__(self, table) -> None:
        self.table = table

    def _get_page_path(self, pages_id: int, col: int, is_base: bool):
        """
        Generates a file path for a given RID.
        """
        page_type = "base" if is_base else "tail"

        return os.path.join(self.table.db_path, f"{page_type}_{pages_id}_{col}.bin")

    def get_page(self, pages_id: int, col: int):
        """
        Reads a 4KB page from disk corresponding to the given RID.
        Returns the page data as bytes.
        """
        page_path = self._get_page_path(pages_id, col)

        # Check if page file exists
        if not os.path.exists(page_path):
            raise FileNotFoundError(f"Page with ID {pages_id} not found on disk.")

        # Read and return page data
        with open(page_path, "rb") as file:
            data = file.read(self.PAGE_SIZE)

        return data
    
    def add_page(self, page: Page, pages_id: int, col: int):
        """
        Writes or updates a 4KB page on disk corresponding to the given RID.
        :param rid: RID to identify the page location.
        :param page_data: Byte data of the page to be written to disk.
        """
        if len(page) != self.PAGE_SIZE:
            raise ValueError("Page data must be exactly 4KB.")

        page_path = self._get_page_path(pages_id, col, page.is_base)

        # Write page data to the file
        with open(page_path, "wb") as file:
            file.write(page)

        print(f"Page with RID {pages_id} written to disk at {page_path}.")

    def write_all_pages(self, pages):
        """
        Sequentially writes all pages in memory to disk.
        :param pages: Dictionary mapping page IDs to page data.
        """
        for page_id, page in pages.items():
            if page.is_dirty:  # write dirty pages
                self.add_page(page_id, page.data)
                page.is_dirty = False  # Mark page as clean
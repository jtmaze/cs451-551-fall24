import os

from lstore import config

from lstore.page import Page
from lstore.storage.meta_col import MetaCol
from lstore.storage.rid import RID

class Disk:
    PAGE_SIZE = config.PAGE_SIZE  # 4KB page size

    def __init__(self, table) -> None:
        self.table = table

    def _get_page_path(self, pages_id: int, col: int):
        """
        Generates a file path for a given RID.
        """
        page_type = "tail" if pages_id % 2 else "base"

        return os.path.join(self.table.db_path,
            f"pages/{page_type}_{pages_id}_{col}.bin")

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

        return Page.from_data(data, pages_id)
    
    def add_page(self, page: Page, pages_id: int, col: int):
        """
        Writes or updates a 4KB page on disk corresponding to the given RID.
        :param rid: RID to identify the page location.
        :param page_data: Byte data of the page to be written to disk.
        """
        # if page.num_records * config.RECORD_SIZE != self.PAGE_SIZE:
        #     raise ValueError("Page data must be exactly 4KB.")
        
        page_path = self._get_page_path(pages_id, col)

        # Write page data to the file
        with open(page_path, "wb") as file:
            file.write(page.data)

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

    def scan_base_records(self, index_cols: list[int]):
        """
        Generates RID & column value pairs where the column values are tuples
        of data values corresponding to the given index columns.
        """
        path = f"{self.table.db_path}/pages/"

        # Get all filepaths for base pages with RIDs
        rid_filepaths = self._get_rid_filepaths(path, is_base=True)

        # Offset index column indices to match page names on disk
        num_metadata_cols = len(MetaCol)
        real_index_cols = [col + num_metadata_cols for col in index_cols]

        # For each RID page
        for rid_path in rid_filepaths:
            pages_id = rid_path.split("_")[1]  # Get pages_id from name

            # Read page from disk
            with open(rid_path, "rb") as file:
                bytes = file.read(self.PAGE_SIZE)
                rid_page = Page.from_data(bytes, pages_id)

            # For each rid, get its corresponding index column data
            for rid in rid_page:
                rid = RID(rid)
                _, offset = rid.get_loc()

                columns = []

                for col in real_index_cols:
                    data_path = os.path.join(path, f"base_{pages_id}_{col}.bin")

                    with open(data_path, "rb") as file:
                        bytes = file.read(self.PAGE_SIZE)
                        data_page = Page.from_data(bytes, pages_id)

                    columns.append(data_page.read(offset))

                yield rid, columns

        # for pages_id, page_entry in self.bufferpool.page_table:
        #     if not page_entry.data[0].is_base:  # Skip tail pages
        #         continue

        #     for offset in range(page_entry.bytes // config.RECORD_SIZE):
        #         rid = RID.from_params(pages_id, offset, is_base=1, tombstone=0)
        #         try:
        #             record = self.get_record(rid, [1] * self.table.num_columns, rel_version=0)
        #             yield rid, record.columns
        #         except Exception as e:
        #             print(f"Error scanning base record {rid}: {e}")

    def _get_rid_filepaths(self, dir, is_base=True):
        """Gets filepaths of base or tail pages for given index columns."""
        page_type = "base" if is_base else "tail"

        with os.scandir(dir) as entries:
            rid_idx_substr = f"_{MetaCol.RID}."

            # Get all filepaths of given type and with a indexed column label
            filepaths = [
                os.path.join(dir, entry.name) for entry in entries if 
                all(substr in entry.name for substr in (page_type, rid_idx_substr))
            ]

        return filepaths
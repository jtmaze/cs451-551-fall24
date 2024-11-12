import os

class Disk:
    PAGE_SIZE = 4096  # 4KB page size
    STORAGE_DIR = "db_storage"  # Directory for storing pages on disk

    def __init__(self) -> None:
        # Ensure the storage directory exists
        os.makedirs(self.STORAGE_DIR, exist_ok=True)

    def _get_page_path(self, rid):
        """
        Generates a file path for a given RID.
        """
        return os.path.join(self.STORAGE_DIR, f"page_{rid.page_id}.bin")

    def get_page(self, rid):
        """
        Reads a 4KB page from disk corresponding to the given RID.
        Returns the page data as bytes.
        """
        page_path = self._get_page_path(rid)

        # Check if page file exists
        if not os.path.exists(page_path):
            raise FileNotFoundError(f"Page with RID {rid} not found on disk.")

        # Read and return page data
        with open(page_path, "rb") as file:
            data = file.read(self.PAGE_SIZE)

        return data
    
    def add_page(self, rid, page_data):
        """
        Writes or updates a 4KB page on disk corresponding to the given RID.
        :param rid: RID to identify the page location.
        :param page_data: Byte data of the page to be written to disk.
        """
        if len(page_data) != self.PAGE_SIZE:
            raise ValueError("Page data must be exactly 4KB.")

        page_path = self._get_page_path(rid)

        # Write page data to the file
        with open(page_path, "wb") as file:
            file.write(page_data)

        print(f"Page with RID {rid} written to disk at {page_path}.")
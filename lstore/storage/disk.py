class Disk:
    def __init__(self) -> None:
        pass

    def get_page(rid):
        pass
    
    def add_page(rid, page):
        pass

    def get_all_page_rids(page):
        """
        Returns all the RIDs on a given a 'page_id'

        TBD, might be unecessary or better to implement in bufferpool.py??
        However, merge_mgr.py probably needs a method like this??
        """
        pass

    def list_all_page_ids(self):
        """
        Lists all the page_ids in the disk.

        Probably uncecessary, becuase bufferpool.py tracks page_ids.
        """

        pass
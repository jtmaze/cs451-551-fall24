"""
# Steps for merge algorithm:
#1. Identify the committed tail records in tail pages
#2. Load the corresponding outdate base pages
#3. Consolidate the base and tail pages
#4. Update the page directory
#5. De-allocate the outdate base pages
"""

from collections import namedtuple

from lstore.storage.buffer.bufferpool import Bufferpool
from lstore.page import Page
from lstore.transaction import Transaction
from lstore.storage.disk import Disk

class MergeManager:
    """
    Returns a new consilidated set of read only base pages.
    """
    def __init__(self, original_tail_pages, original_base_pages):

        pass

    def load_committed_pages(self, page_ids, is_base=False):
        """
        Returns all the records and designates them as base or committed tail records.  
        """

        records = self._get_records(page_ids)
        for rid, record in records.items():
            if is_base:
                record.set_base()
            else:
                record.set_tail()

        return records
    
    def merge_pages(self):
        """
        Conceuptually, a left-outer join
        Inputs: a list of base page_ids and a list of tail page_ids
        Returns: a dictionary with read only base pages.
        """
        # How to get the base and tail page_ids???
        original_base_records = self.load_committed_pages(self.base_page_ids, is_base=True) 
        committed_tail_records = self.load_committed_pages(self.tail_page_ids, is_base=False)
        

        new_base_records = {}

        return new_base_records
    
    def update_page_directory(self):
        """
        Rearranges pointers in the page directory (record_index.py?)
        This is the only 'foreground' operation in merge manager.
        """

        new_page_directory = namedtuple()

        return new_page_directory
    
    def deallocate_base_pages(self):
        """
        For now, this method will just swap the old base pages with the new base pages.
        For M3, things becoeme more complicated, becuase there can be an active query on out-date base pages.
        """
        pass

    # Helpers ----------------------------

    def _get_records(self, pages):
        """
        Inputs: a list of page_ids
        Returns: All the records in the list of pages
        """

        records = {}
        for page in pages:
            records.update(self._get_page_records(page))
        
        return records

    def _get_page_records(self, page):
        """
        Inputs: a single page_id
        Returns: all the committed records for a given page_id
        """

        # How to get all RIDs for a given page? Should this method be in page.py or disk.py?
        # Probably a method in page.py, because it keeps each page modular with its own RIDs. 
        page_rids = page.get_all_page_rids() 
        
        page_records = {}
        for i, rid in enumerate(page_rids):
            record = page.read(i) # Uses the read method from bufferpool.py??

            # Check the committed status of the record only return committed records
            commit_status = self._check_committed_status(record)
            if commit_status == True:
                page_records[rid] = record
            else:
                pass 

        return page_records
    
    def _check_committed_status(self, record):
        """
        Checks the committed status of a record.
        If were're only getting RIDs with disk.py, this may not be necessary??
        """
        if record.is_base():
            # Aren't all base records committed?
            return True 
        elif record.is_tail():
            if record.is_committed(): # Would this use transaction.py's commit method???
                return True
            else:
                return False
        else:
            raise Exception("Need to designate record as base OR Tail.")




"""
Steps to the Merge Algorithm as we're implementing it:
1. Get a batch of base pages could be from disk or buffer (buffer is probably faster)
2. Get corresponding tail pages for the base RIDs. 
3. Consolidate base records with update tail records
4. Update the page directory with the new base records
5. De-allocate the outdated base pages

# Stupid/simple questions:
- How do we set up a "background thread" to run the merge manager?
    - Do we use conncurrent.futures?
- Also, how can I make the page directory modifacation work in the foreground?


# Less stupid questions:
- Granularity of the merge operation: 
    - Easiest approach seems coarser-grained and less frequent merges?
    - Is this a question of how many committed tail records we fetch in each batch??

- When loading corresponding base pages, use the get_page method from disk.py, or go from buffer pool??


"""
from collections import namedtuple

import concurrent.futures # For running background thread
import time # For how often we shold merge. 

from lstore.storage.buffer.bufferpool import Bufferpool
from lstore.storage.disk import Disk
from lstore.storage.buffer.buffer import Buffer
from lstore.storage.rid import RID

from lstore.storage.record import Record
from lstore.storage.buffer.page_table import PageTable

from lstore.storage.meta_col import MetaCol

class MergeManager:
    """
    Returns a new consolidated set of read only base pages.

    """
    def __init__(self, buffer, page_table, disk):
        self.buffer = buffer
        self.bufferpool = buffer.bufferpool
        self.page_table = page_table
        self.disk = disk

        self.merge_threshold = None # Set merge threshold in config.py??
        self.all_base_page_ids = self.get_all_base_page_ids()
        #self.all_tail_page_ids = self.get_all_tail_page_ids()
        self.batch_size = 1 # Set batch size in config.py??

    def get_all_base_page_ids(self):
        """
        Inputs: The page table
        Returns: A list of all the base page IDs
        """
        return [page_id for page_id in self.page_table.ptable if page_id % 2 ==0]

    # def get_all_tail_page_ids(self):
    #     """
    #     Inputs: The page table
    #     Returns: A list of all the tail page IDs
    #     """
    #     return [page_id for page_id in self.page_table.ptable if page_id % 2 != 0]

    def merge(self, all_base_page_ids, batch_size):

        """
        Iterates the merge operation for a given batch size.
        """
        total_base_pages = len(all_base_page_ids)

        for i in range(0, total_base_pages, batch_size):
        
            base_page_paths = self.basepages_to_merge_que(all_base_page_ids, start=i, batch_size=batch_size)
            # Load the base records and corresponding tail records
            base_records: list[Record] = self.load_base_records(base_page_paths)
            updated_tails = self.find_latest_tail_records(base_records)

            # Merge the records
            new_base_records = self.merge_records(base_records, updated_tails)

            # Update the page directory / Write the new pages.
            new_base_records.update_page_directory()

            # Deallocate the base old pages
            self.delete_old_base_pages(base_page_paths)

            i += batch_size


    def basepages_to_merge_que(self, base_page_ids, start=0, batch_size=None):
        """
        Inputs: Every base page_ids
        Returns: List of base page paths for the merge operation.
        """

        page_table: PageTable = self.page_table
        subst_ids = base_page_ids[start:start + batch_size]
        page_paths = []

        for page_id in subst_ids:
            page_entry = self.page_table.get_pages(page_id)
            if page_entry:
                for col in range(len(page_entry)):
                    path = self.buffer.disk._get_page_path(page_id, col, 0) # TODO: Should the offset argument be = 0??
                    print(path)
                    page_paths.append(path)
        
        return page_paths

    def load_specific_base_records(self, page_paths):
        """
        Inputs: A list of paths to all the base pages in the batch/subset 
        Returns: A list of tuples with all of the base records. 
        """

        #disk: Disk = self.disk
        page_table: PageTable = self.page_table  

        base_records = []
        tcols = self.page_table.tcols

        for page_path in page_paths:
            loaded_pages = []
            for col in range(tcols):
                # Assmuning buffer has a method returning a page given a path. 
                page = self.disk.get_page(page_path, col)


        return base_records

    
    def find_latest_tail_records(self, base_records: list[Record]) -> dict[RID, Record]:
        """
        Inputs: A list of base records (as tuples?)
        Returns: The most up-to-date tail record for each base record's RID
        """

        disk: Disk = self.disk

        updated_tails = {}

        for r in base_records:

            base_rid = r.rid # OR r[0] OR r['RID']... How to get the RID from the record???

            matched_tail_page_id = f''

            #
            # This tail page is returned as bytes, not as records (e.g. a list of tuples)
            tail_page = disk.get_page(pages_id= , col= , offset= ) 
            tail_record: Record = disk.get_record(base_rid, projected_col_idx=[], rel_version=0)
            # Add base RID to tail records. This helps track base records in the merge_records() method. Then, drop it after the merge. 
            tail_record = tail_record.update({'base_rid': base_rid})
            # Add each tail record to the updated_tails dictionary
            updated_tails[base_rid] = tail_record

        return updated_tails
    
    def merge_records(self, base_records, updated_tails):
        """
        Conceuptually, a left-outer join
        Inputs: Two dictionaries with base records and updated tail records.
        Returns: Dictionary with the updated base records.
        """

        new_base_records = base_records.copy()

        for record in updated_tails:
            base_rid = record.pop('base_rid')
            if base_rid not in new_base_records:
                pass
            else:
                new_base_records[base_rid] = record

        return new_base_records
    

    def update_page_directory(self):
        """
        
        This is the only 'foreground' operation in merge manager.
        """

        new_page_directory = 'huh'

        return new_page_directory
    
    def deallocate_base_pages(self):
        """
        Deallocates the old base base pages after they're merged.
        """
        pass

    # Helpers ----------------------------

    def _get_records(self, page_paths) -> list[Record]:
        """
        Inputs: a list of page paths
        Returns: All the records in the list of pages
        """

        records = []
        for page_path in page_paths:
            page = read_page(page_path)
            records.append(self._get_page_records(page))
        
        return records

    def _get_page_records(self, page):
        """
        Inputs: a single page_id
        Returns: all the committed records for a given page_id
        """
        buffer = self.buffer

        # How to get all RIDs for a given page? Should this method be in page.py or disk.py?
        # Probably a method in page.py, because it keeps each page modular with its own RIDs. 
        page_rids = page.get_all_page_rids() 
        
        page_records = {}
        for i, rid in enumerate(page_rids):
            record = (i) # Uses the read method from bufferpool.py??


        return page_records
    

# ----- Garbage Code -----

    # def get_all_tail_page_ids(self):
    #     """
    #     Inputs: The page table
    #     Returns: A list of all the tail page IDs
    #     """
    #     return [page_id for page_id in self.page_table.ptable if page_id % 2 != 0]




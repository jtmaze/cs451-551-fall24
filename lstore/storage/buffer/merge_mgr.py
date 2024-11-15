"""
Steps to the Merge Algorithm as we're implementing it:
1. Get a batch of base pages.
2. Use the inderection pointers to get tail pages with the latest updates. 
3. 

Steps for merge algorithm from Lstore paper:
1. Identify the committed tail records in tail pages
2. Load the corresponding outdated base pages
3. Consolidate the base and tail pages
4. Update the page directory
5. De-allocate the outdated base pages

# Stupid/simple questions:
- By "committed tail records" do we just mean tail records on disk???
- How do we set up a "background thread" to run the merge manager?
    - Do we use conncurrent.futures?
- Also, how can I make the page directory modifacation work in the foreground?
- Unless I'm wrong, every RID has their own page on disk.py? 


# Less stupid questions:
- Granularity of the merge operation: 
    - Easiest approach seems coarser-grained and less frequent merges?
    - Is this a question of how many committed tail records we fetch in each batch??

- When loading corresponding base pages, use the get_page method from disk.py??

- How can we load the tail-records in descending order of update time? This makes the merge algorithm more efficient.
    - *** If RID monotonically decreases for a given base record with update time, could be beneficial. 

"""
# %%

# %%

from collections import namedtuple

import concurrent.futures # For running background thread
import time # For how often we shold merge. 

from lstore.storage.buffer.bufferpool import Bufferpool
from lstore.page import Page
from lstore.transaction import Transaction
from lstore.storage.disk import Disk
from lstore.storage.buffer.buffer import Buffer
from lstore.storage.rid import RID

from lstore.storage.record import Record

from lstore.storage.meta_col import MetaCol

class MergeManager:
    """
    Returns a new consolidated set of read only base pages.
    """
    def __init__(self, buffer, all_base_page_ids):
        self.buffer = buffer

        self.merge_threshold = None # Set merge threshold in config.py??
        self.all_base_page_ids = all_base_page_ids
        self.batch_size = 1 # Set batch size in config.py??


        pass

    def merge(self, all_base_page_ids, batch_size):

        """
        Runs the merge operation
        """
        total_base_pages = len(all_base_page_ids)

        for i in range(0, total_base_pages, batch_size):
        
            base_page_paths = self.basepages_to_merge_que(all_base_page_ids, start=i, n_pages=n_pages)
            # Load the base records and corresponding tail records
            base_records: list[Record] = self.load_base_records(base_page_paths)
            updated_tails = self.find_latest_tail_records(base_records)

            # Merge the records
            new_base_records = self.merge_records(base_records, updated_tails)

            # Update the page directory / Write the new pages.
            new_base_records.update_page_directory()

            # Deallocate the base old pages
            base_page_paths.delete_old_base_pages()

            i += batch_size


    def basepages_to_merge_que(self, base_page_ids, start=0, n_pages=n_pages):
        """
        Inputs: Every base page_ids
        Returns: List of base page paths for the merge operation.
        """

        for i in range(start, len(base_page_ids), n_pages):
            # TODO: Method for generating page paths on disk from page_ids
            subset = self._get_paths_from_page_ids(base_page_ids[i:i + n_pages])

            return subset

    def load_base_records(self, page_pointers):
        """
        Inputs: A list of paths to subset of base pages
        Returns: A dictionary with all base records.
        """

        base_records = self._get_records(page_pointers)
        return base_records
    
    def find_latest_tail_records(self, base_records: list[Record]) -> dict[RID, Record]:
        """
        Inputs: A dictionary with base records 
        Returns: The most up-to-date tail record for each base RID
        """

        buffer: Buffer = self.buffer

        updated_tails = {}

        for r in base_records:
            # new_vals[MetaCol.INDIR] = int(tail_rid)
            # new_vals[MetaCol.RID] = int(rid)
            # new_vals[MetaCol.SCHEMA] = 0
            base_rid = r.rid # OR r[0] OR r['RID']... How to get the RID from the record???
            # How to handle the projected_col_idx arg???
            tail_record: Record = buffer.get_record(base_rid, projected_col_idx=[], rel_version=0)
            # Add base RID to tail record, drop after the merge.
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
    

    # ---------------------------- Garbage

        # def _check_committed_status(self, record):
    #     """
    #     Checks the committed status of a record.
    #     If were're only getting RIDs with disk.py, this may not be necessary??
    #     """
    #     if record.is_base():
    #         # Aren't all base records committed?
    #         return True 
    #     elif record.is_tail():
    #         if record.is_committed(): # Would this use transaction.py's commit method???
    #             return True
    #         else:
    #             return False
    #     else:
    #         raise Exception("Need to designate record as base OR Tail.")

        # def _get_base_pages(self, base_page_rids):
    #     """
    #     Inputs: a list of RIDs for base pages
    #     Returns: a list of read pages as bytes
    #     """
    #     disk = Disk()
    #     base_pages = [] # List data structure might be a bad idea here
    #     for rid in base_page_rids:
    #         base_pages= [disk.get_page(rid) for rid in base_page_rids]

    #     return base_pages




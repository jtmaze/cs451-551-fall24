"""
# Steps for merge algorithm:
#1. Identify the committed tail records in tail pages
#2. Load the corresponding outdated base pages
#3. Consolidate the base and tail pages
#4. Update the page directory
#5. De-allocate the outdated base pages

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

from collections import namedtuple
import concurrent.futures # For running background thread
import time # For how often we shold merge. 

from lstore.storage.buffer.bufferpool import Bufferpool
from lstore.page import Page
from lstore.transaction import Transaction
from lstore.storage.disk import Disk

class MergeManager:
    """
    Returns a new consolidated set of read only base pages.
    """
    def __init__(self, original_tail_pages, original_base_pages):

        self.merge_threshold = None # Set merge threshold in config.py??

        pass

    def tailpages_to_merge_que(self, tail_page_ids, start=0, n_pages=100):
        """
        Inputs: Full list of tail page_ids ()
        Returns: List of n tail page_ids to for the merge operation.
        """

        for i in range(start, len(tail_page_ids), n_pages):
            # TODO: Method for generating tailpage paths
            subset = self._get_paths_from_page_ids(tail_page_ids[i:i + n_pages])

            return subset

    def load_pages(self, page_pointers, is_base=False):
        """
        Inputs: page_pointers - either RIDs (base pages) or page_ids (tail pages)
                - If base_page=False a list of file paths to pages
                - If base_page=True a list of base page RIDs
        Returns: A dictionary with records for every page
        """

        if is_base:
            # A list of base pages as bytes, should probably convert to tuples
            base_pages = self._get_base_pages(page_pointers)
            records = base_pages._get_page_records(base_pages)
        else:
            records = self._get_page_records(page_pointers)

        return records
    
    def find_latest_tail(records):
        """
        Inputs: A dictionary with all the tail records (spans multiple base RIDs)
        Returns: A dictionary with the most up-to-date record for each base RID
        """
        
        # 1. 
        # From the tail records, we need a way to find the number of unique by base RIDs (think primary key..) for the records. 
        # Seems similar to the _get_base_indicies() helper in bufferpool.py

        # 2. 
        # For each unique base RID, find the latest tail record
        # The rel_version should = 0
        pass

    def merge_pages(self, tail_page_ids, base_page_RIDs, start=0):
        """
        Conceuptually, a left-outer join
        Inputs: a list of base page_ids and a list of tail page_ids
        Returns: a dictionary with updated read only base pages.
        """
        # How to get the base and tail page_ids???
        tail_page_ids_subset = self.tailpages_to_merge_que(tail_page_ids, start=start)
        tail_records = self.load_pages(self.tail_page_ids_subset, is_base=False)
        latest_tails = self.find_latest_tail(tail_records)


        # Need tail pages RIDs and fetch base records from disk based on that??
        original_base_records = self.load_pages(self.base_page_RIDs, is_base=True) 
        new_base_records = original_base_records.copy() 

        latest_tail_records = {}

        # Find the latest tail record for each base RID
        for tail_rid in reversed(sorted(self.tail_records.keys())):
            tail_record = self.tail_records[tail_rid]
            base_rid = tail_record['base_rid'] # TODO: Method to get base RID from tail record's indirection if not stored. 
            if base_rid not in latest_tail_records:
                latest_tail_records[base_rid] = tail_record

        # Apply the latest tail record to the base record
        for base_rid, base_record in self.base_records.items():
            if base_rid not in new_base_records:
                tail_record = latest_tail_records[base_rid]
                new_base_record = self.apply_tail_to_base(new_base_records[base_rid], tail_record)
                new_base_records[base_rid] = new_base_record
            else:
                pass

                
        return new_base_records
    
    def apply_tail_to_base(self, base_record, tail_record):
        """
        Inputs: a base record and the latest corresponding tail record
        Returns: a new base record with the tail record applied.
        """

        new_base_record = base_record.copy() 
        schema_encoding = tail_record['schema_encoding']

        for i, column_changed in enumerate(schema_encoding):
            if column_changed == 1:
                new_base_record['columns'][i] = tail_record['columns'][i]

        new_base_record['last_update_time'] = tail_record['last_update_time']
        new_base_record['schema_encoding'] = tail_record['schema_encoding']

        return new_base_record


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
        Deletes the old base pages
        """
        pass

    # Helpers ----------------------------

    def _get_records(self, page_paths):
        """
        Inputs: a list of page paths
        Returns: All the records in the list of pages
        """

        records = {}
        for page in page_paths:
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
    
    def _get_base_pages(self, base_page_rids):
        """
        Inputs: a list of RIDs for base pages
        Returns: a list of read pages as bytes
        """
        disk = Disk()
        base_pages = [] # List data structure might be a bad idea here
        for rid in base_page_rids:
            base_pages= [disk.get_page(rid) for rid in base_page_rids]

        return base_pages
    
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

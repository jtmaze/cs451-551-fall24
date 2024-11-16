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
from collections import defaultdict

import os
import time # For how often we shold merge. 

from lstore import config

from lstore.page import Page
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
    def __init__(self, table):
        self.table = table

        self.tcols = table.num_total_cols

        #self.all_tail_page_ids = self.get_all_tail_page_ids()

    def merge(self):
        """
        Iterates the merge operation for a given batch size.
        """
        # Get all base page ids IN MEMORY
        base_page_ids = self._get_base_page_ids()

        batch_size = config.MERGE_BATCH_SIZE

        # base_paths = self._get_page_paths(mem_base_ids)

        for i in range(0, len(base_page_ids), batch_size):
            # base_page_paths = self._get_batch_paths(
            #     mem_base_ids, start=i, batch_size=batch_size)
            # batch_paths = base_paths[i:i + batch_size]
            batch_ids = base_page_ids[i:i + batch_size]

            # Load the base records and corresponding tail records
            base_data: list[tuple[int]] = self._find_base_records(batch_ids)
            updated_tails = self._find_latest_tail_records(base_data)

            # Merge the records
            # new_base_records = self.merge_records(base_data, updated_tails)

            # Update the page directory / Write the new pages.
            # new_base_records.update_page_directory()

            # Deallocate the base old pages
            # self.delete_old_base_pages(base_page_paths)

    def _get_base_page_ids(self):
        """
        Gets list of all base page ids IN MEMORY.

        Inputs: The page table
        Returns: A list of all the base page IDs
        """
        page_table = self.table.buffer.bufferpool.page_table

        return [page_id for page_id in page_table if page_id % 2 == 0]
    
    def _get_page_paths(self, page_ids):
        paths = []

        for page_id in page_ids:
            for col in range(self.table.num_total_cols):
                filename = f"base_{page_id}_{col}.bin"
                filepath = os.path.join(self.table.db_path, "pages/", filename)
                paths.append(filepath)

        return paths

    def _find_base_records(self, page_ids: list[int]) -> list[tuple[int]]:
        """
        Inputs: A list of paths to all the base pages in the batch/subset 
        Returns: A list of tuples with all of the base records. 
        """
        page_table: PageTable = self.table.buffer.bufferpool.page_table
        disk: Disk = self.table.disk
        tcols = self.tcols

        base_data = []

        for page_id in page_ids:
            # Get all pages associated with page id
            pages = []
            for col in range(tcols):
                page: Page = page_table.get_page(page_id, col)
                if page is None:
                    page = disk.get_page(page_id, col)
                pages.append(page)

            # Get all rids in page
            rid_page = pages[MetaCol.RID]
            rids = [RID(rid) for rid in rid_page]

            # Get all data for each rid associated with page id
            for rid in rids:
                _, offset = rid.get_loc()

                base_tuple = tuple(page.read(offset) for page in pages)

                base_data.append(base_tuple)

        return base_data

    def _find_latest_tail_records(self, base_data: list[tuple[int]]) -> dict[RID, Record]:
        """
        Inputs: A list of base records (as tuples?)
        Returns: The most up-to-date tail record (on disk) for each base record's RID
        """
        def _get_tail_tuple():
            tail_tuple = []
            # Fetch the page for every column in the table
            for col in range(tcols):
                page = temp_table[page_id][col]
                # Check if page is already in memory
                if page is None:
                    try: # If not, fetch it from disk
                        page = disk.get_page(page_id, col)
                        temp_table[page_id][col] = page
                    except FileNotFoundError:
                        return None

                tail_tuple.append(page.read(offset))
            # Tuple with all columns for a given tail RID
            return tail_tuple

        disk: Disk = self.table.disk
        tcols = self.tcols

        tail_data = dict()

        temp_table = defaultdict(lambda: [None for _ in range(tcols)])

        for base_tuple in base_data:
            indir = RID(base_tuple[MetaCol.INDIR])
            page_id, offset = indir.get_loc()

            tail_tuple = _get_tail_tuple()
            # Handles case tails aren't on disk for a given base record. Could be no writes yet, or on committed writes. 
            if tail_tuple is None:
                continue

            tail_data[base_tuple[MetaCol.RID]] = _get_tail_tuple()

        return tail_data
        
    # def merge_records(self, base_records: list[tuple[int]], updated_tails: dict[RID, tuple[int]]) -> dict:
    #     """
    #     Conceuptually, a left-outer join
    #     Inputs: Original base data (list of tuples) and most up-to-date tail records (dict)
    #     Returns: ?Dictionary? with the updated base records.
    #     """
    #     tcols = self.tcols

    #     # base_data =
    #     new_base_records = base_records.copy()

<<<<<<< Updated upstream
    #     for base_record in base_records:

    #         base_rid_value = base_record[MetaCol.RID]
    #         updated_record = list(base_record)

    #         for col_idx in range(, tcols):
    #             )            

    #     return new_base_records
=======
            base_rid = base_record[MetaCol.RID]
            
            if base_rid in updated_tails:
                # Merge record should be identical to the corresponding updated tail
                merge_record = updated_tails[base_rid] 
                new_base_records.append(merge_record)
            else:
                new_base_records.append(base_record)

        return new_base_records
>>>>>>> Stashed changes

    def update_page_directory(self):
        """
        
        This is the only 'foreground' operation in merge manager.
        """

        new_page_directory = 'huh'

        return new_page_directory
    
    def delete_old_base_pages(self, base_page_paths):
        
        pass
    
    # Helpers ----------------------------


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


    # def _get_batch_paths(self, base_page_ids, start=0, batch_size=None):
    #     """
    #     Inputs: Every base page_ids
    #     Returns: List of base page paths for the merge operation.
    #     """
    #     page_table: PageTable = self.page_table
    #     subst_ids = base_page_ids[start:start + batch_size]
    #     page_paths = []

    #     for page_id in subst_ids:
    #         page_entry = self.page_table.get_pages(page_id)
    #         if page_entry:
    #             for col in range(len(page_entry)):
    #                 path = self.buffer.disk._get_page_path(page_id, col, 0) # TODO: Should the offset argument be = 0??
    #                 print(path)
    #                 page_paths.append(path)




"""
Unit tests for OLTP operations (insert, select, update, delete)
"""

import sys
import os

# Add root dir to path to find lstore
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# -----------------------

from typing import Literal

import copy
import random

from lstore.db import Database
from lstore.query import Query
from lstore.table import Record

from lstore.index_types.index_config import IndexConfig
from lstore.index_types.bptree import BPTreeIndex
from lstore.index_types.dict_index import DictIndex

import test_util

class TestOLTP:
    # Config
    max_time = 5.0 # Test fails if operation takes longer (seconds)
    num_records = 10_000
    data_cols = 12
    total_cols = data_cols + 1
    val_range = (-100, 100)
    agg_range = (1000, 2000) # Range of keys to aggregate
    agg_col_idx = 4          # Index of column to aggregate
    version_count = 3 # Number of updates for version testing (NOT INCLUDING BASE & 1ST UPDATE)
    index_config = IndexConfig() # Specify index?

    # Class level initialization
    db = Database()
    table = db.create_table("TestTable", data_cols + 1, 0, index_config)
    query = Query(table)
    random.seed(42)
    records = test_util.gen_records(num_records, data_cols, val_range)
    base_records = copy.deepcopy(records)

    def test_insert(self):
        insert_time = 0.0
        timed_insert = test_util.timeit(self.query.insert)

        for columns in self.records.values():
            _, t_diff = timed_insert(*columns)
            insert_time += t_diff
            
        print(f"Inserting {len(self.records):,} took {insert_time:.4f}s")
        print("Insert finished\n")

        assert insert_time < self.max_time

    def test_select(self):
        select_time = 0.0
        timed_select = test_util.timeit(self.query.select)

        for key, record in self.records.items():
            proj_col_idx = [random.choice([0, 1]) for _ in range(self.total_cols)]

            result, t_diff = timed_select(key, 0, proj_col_idx)
            select_time += t_diff

            # Get record from returned list (point self.query)
            result: Record | bool = result[0]

            # Check correctness
            j = 0 # Column index for projected result
            for i, include in enumerate(proj_col_idx):
                if not include:
                    continue

                assert result.columns[j] == record[i]
                j += 1

        print(f"Selecting {len(self.records):,} took {select_time:.4f}s")
        print("Select finished\n")

        assert select_time < self.max_time

    def test_update_random(self):   
        update_time = 0.0
        timed_update = test_util.timeit(self.query.update)

        for key, record in self.records.items():
            # Randomly choose columns to update and change record
            update_vals = self._update_record_random(record)

            _, t_diff = timed_update(key, *update_vals)
            update_time += t_diff

            # Check correctness
            new_record = self.query.select(key, 0, [1 for _ in range(self.total_cols)])[0]

            assert new_record.columns == record
            
        assert update_time < self.max_time

    def test_select_version(self):
        v_count = self.version_count

        op_time = 0.0
        timed_update = test_util.timeit(self.query.update)
        timed_sv = test_util.timeit(self.query.select_version)

        proj_col_idx = [1 for _ in range(self.total_cols)]

        real_history = [[0 for _ in range(self.total_cols)] for _ in range(v_count)]

        for key, record in self.records.items():
            # Perform updates while tracking history
            for i in range(v_count):
                # Overwrite ground truth history with current version
                real_history[i][:] = record

                # Update database (add tail record)
                update_vals = self._update_record_random(record)
                _, t_diff = timed_update(key, *update_vals)
                op_time += t_diff

            # Select versions while checking for correctness vs history
            for i in range(1, v_count + 1):
                result, t_diff = timed_sv(key, 0, proj_col_idx, -i)
                op_time += t_diff
                assert result[0].columns == real_history[-i]

            # Check base record as well (kept through original update test)
            result, t_diff = timed_sv(key, 0, proj_col_idx, -(v_count + 1))
            op_time += t_diff
            assert result[0].columns == self.base_records[key]
            
        assert op_time < self.max_time


    def test_sum(self):
        start_range, end_range = self.agg_range # Start of key range

        timed_sum = test_util.timeit(self.query.sum)

        result, t_diff = timed_sum(start_range, end_range, self.agg_col_idx)

        # Check correctness
        real_sum = 0.0
        for i in range(start_range, end_range + 1):
            real_sum += self.records[i][self.agg_col_idx]

        assert result == real_sum
            
        assert t_diff < self.max_time

    def test_sum_version(self):
        sumv_time = 0.0
        timed_sumv = test_util.timeit(self.query.sum_version)

        for col in range(self.total_cols):
            # Test most recent record
            result, t_diff = timed_sumv(*self.agg_range, col, 0)
            sumv_time += t_diff
            real_sum = 0
            for i in range(self.agg_range[0], self.agg_range[1] + 1):
                real_sum += self.records[i][col]
            assert result == real_sum

            # Test base record
            result, t_diff = timed_sumv(*self.agg_range, col, -(self.version_count + 1))
            sumv_time += t_diff
            real_sum = 0
            for i in range(self.agg_range[0], self.agg_range[1] + 1):
                real_sum += self.base_records[i][col]
            assert result == real_sum

        assert sumv_time < self.max_time

    def test_delete(self):
        delete_time = 0.0
        timed_delete = test_util.timeit(self.query.delete)

        for key in self.records:
            _, t_diff = timed_delete(key)
            delete_time += t_diff

            # Correctness check
            del_record = self.query.select(key, 0, [1 for _ in range(self.total_cols)])
            assert not del_record
            
        assert delete_time < self.max_time

    # Helpers -------------

    def _update_record_random(self, record: list[int]):
        # Randomly choose columns to update
        update_vals = [None for _ in range(self.total_cols)]
        update_idx = random.sample(range(1, self.total_cols), self.data_cols)

        # Get random number to update to for each column
        for idx in update_idx:
            new_val = random.randint(*self.val_range)
            record[idx] = new_val
            update_vals[idx] = new_val
    
        return update_vals
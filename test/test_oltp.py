"""
Unit tests for OLTP operations (insert, select, update, delete)
"""

import sys
import os

# Add root dir to path to find lstore
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# -----------------------

from typing import Literal

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
    max_time = 10.0

    num_records = 50_000
    data_cols = 12
    total_cols = data_cols + 1
    val_range = (-100, 100)

    index_config = IndexConfig(DictIndex)

    # Class level initialization
    db = Database()
    table = db.create_table("TestTable", data_cols + 1, 0, index_config)
    query = Query(table)

    random.seed(42)

    records = test_util.gen_records(num_records, data_cols, val_range)

    def test_insert(self):
        print(self.records)
        insert_time = 0.0

        timed_insert = test_util.timeit(self.query.insert)

        print("Beginning insertion...")

        for columns in self.records.values():
            _, t_diff = timed_insert(*columns)
            insert_time += t_diff
            
        print(f"Inserting {len(self.records):,} took {insert_time:.4f}s")
        print("Insert finished\n")

        assert insert_time < self.max_time

    def test_select(self):
        proj_col_idx: list[Literal[0, 1]] = None
        ####

        if proj_col_idx is None:
            proj_col_idx = [1 for _ in range(self.total_cols)]
        elif len(proj_col_idx) != self.total_cols:
            raise ValueError("Column projection shape does not match number of columns")

        select_time = 0.0
        timed_select = test_util.timeit(self.query.select)

        for key, record in self.records.items():
            result, t_diff = timed_select(key, 0, proj_col_idx)
            select_time += t_diff

            # Get record from returned list (point self.query)
            result: Record = result[0]

            # Check correctness
            j = 0 # Column index for projected result
            for i, include in enumerate(proj_col_idx):
                if not include:
                    continue

                if result.columns[j] != record[i]:
                    print(f'Error column {i + 1} does not match {self.records}')
                j += 1

        print(f"Selecting {len(self.records):,} took {select_time:.4f}s")
        print("Select finished\n")

        assert select_time < self.max_time


    def test_update_random(self):   
        update_cols: int = None
        gen_fn=random.uniform
        gen_params=(0, 20)
        ####

        total_cols = self.total_cols

        # Update all columns if specific number not given
        if update_cols is None:
            update_cols = self.data_cols

        update_time = 0.0
        timed_update = test_util.timeit(self.query.update)

        print("Beginning update...")

        for key, record in self.records.items():
            update_vals = [None for _ in range(total_cols)]

            # Randomly choose columns to update
            update_idx = random.sample(range(1, total_cols), update_cols)

            # Get random number to update to for each column
            for idx in update_idx:
                new_val = int(round(gen_fn(*gen_params), 0))
                update_vals[idx] = new_val
                record[idx] = new_val

            _, t_diff = timed_update(key, *update_vals)
            update_time += t_diff

            # Check correctness
            new_record = self.query.select(key, 0, [1 for _ in range(total_cols)])[0]
            if new_record.columns != record:
                print(f"Update error: {new_record.columns} != {record}")
            
        print(f"Updating {len(self.records):,} took {update_time:.4f}s")
        print("Update finished\n")

        assert update_time < self.max_time

    def test_sum(self):
        start_range = 1000 # Start of key range
        end_range = 2000   # End of key range
        agg_col_idx = 4  # Index of column to aggregate
        ###

        timed_sum = test_util.timeit(self.query.sum)

        print(f"Beginning summation for keys {start_range} to {end_range}...")

        result, t_diff = timed_sum(start_range, end_range, agg_col_idx)

        # Check correctness
        real_sum = 0.0
        for i in range(start_range, end_range + 1):
            real_sum += self.records[i][agg_col_idx]

        assert result == real_sum
            
        assert t_diff < self.max_time

    def test_delete(self):
        delete_time = 0.0

        timed_delete = test_util.timeit(self.query.delete)

        print("Beginning deletion...")

        for key in self.records:
            _, t_diff = timed_delete(key)
            delete_time += t_diff

            del_record = self.query.select(key, 0, [1 for _ in range(self.total_cols)])

            # Correctness check
            if del_record:
                del_record = del_record[0]
                
                print(f"Delete error: record {del_record.columns} still lives")
            
        print(f"Deleting {len(self.records):,} took {delete_time:.4f}s")
        print("Delete finished\n")

        assert delete_time < self.max_time
        
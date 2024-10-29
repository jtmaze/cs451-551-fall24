from typing import Literal

import time
import random

from lstore.table import Record
from lstore.query import Query

def _timeit(fn):
    def wrapper(*args, **kwargs):
        t0 = time.process_time()
        result = fn(*args, **kwargs)
        t1 = time.process_time()

        return result, t1 - t0
    return wrapper

def create_records(num_columns, num_records, val_range=(0, 20)):
    records = dict()

    keys = list(range(num_records))
    key_range = (keys[0], keys[-1])
    
    random.shuffle(keys)

    for key in keys:
        records[key] = [key] + [
            random.randint(*val_range) for _ in range(num_columns)
        ]

    return records, key_range

def test_insert(query: Query, records: dict[int, tuple[int]]):
    insert_time = 0.0

    timed_insert = _timeit(query.insert)

    print("Beginning insertion...")

    for columns in records.values():
        _, t_diff = timed_insert(*columns)
        insert_time += t_diff
        
    print(f'Inserting {len(records)} took {insert_time}')
    print("Insert finished")

    return insert_time

def test_select(
        query: Query,
        records: dict[int, tuple[int]],
        proj_col_idx: list[Literal[0, 1]]
):
    # if len(proj_col_idx) != len(records[0]) - 1:
    #     raise ValueError("Column projection shape does not match number of columns")

    select_time = 0.0
    timed_select = _timeit(query.select)

    for key, record in records.items():
        result, t_diff = timed_select(key, 0, proj_col_idx)
        select_time += t_diff

        # Get record from returned list (point query)
        result: Record = result[0]

        for i, include in enumerate(proj_col_idx):
            if not include:
                continue

            if result.columns[i] != record[i]:
                print(f'Error column {i + 1} does not match {records}')

    print(f'Selecting {len(records)} took {select_time}')
    print("Select finished")

    return select_time


def test_update(
    query: Query,
    records: dict[int, tuple[int]], 
    num_cols_to_update: int = None,
    val_range=(0, 20)
):   
    """
    """

    total_cols = len(records.values()[0])

    if num_cols_to_update is None:
        num_cols_to_update = total_cols

    update_idx = random.sample(range(1, total_cols), num_cols_to_update)

    update_time = 0.0

    timed_update = _timeit(query.update)

    print("Beginning update...")

    for _ in records:
        update_vals = [None for _ in range(total_cols)]

        # For each column to be updated
        for idx in update_idx:
            update_vals[idx] = random.randint(*val_range)

        _, t_diff = timed_update(0, *update_vals)
        update_time += t_diff
        
    print(f'Updating {len(records)} took {update_time}')
    print("Update finished")

    return update_time

def test_sum(
    query: Query,
    records: dict[int, tuple[int]], 
    start_range: int, # Start of key range
    end_range: int,   # End of key range
    agg_col_idx: int  # Index of column to aggregate
):
    sum_time = 0.0

    

    timed_sum = _timeit(query.sum)

    print("Beginning summation...")

    for columns in records.values():
        _, t_diff = timed_sum(start_range, end_range, agg_col_idx)
        sum_time += t_diff
        
    print(f'Summing {len(records)} took {sum_time}')
    print("Sum finished")

    return sum_time

def test_delete(
    query: Query,
):
    pass
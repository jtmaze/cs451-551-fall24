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

def _get_total_cols(records):
    return len(next(iter(records.values())))

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

def test_insert(query: Query, records: dict[int, list[int]]):
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
        records: dict[int, list[int]],
        proj_col_idx: list[Literal[0, 1]] = None
):
    total_cols = _get_total_cols(records)

    if proj_col_idx is None:
        proj_col_idx = [1 for _ in range(total_cols)]
    elif len(proj_col_idx) != total_cols:
        raise ValueError("Column projection shape does not match number of columns")

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


def test_update_random(
    query: Query,
    records: dict[int, list[int]], 
    num_cols_to_update: int = None,
    val_range=(0, 20)
):   
    """
    Randomly chooses a given number of columns to updates values for.
    
    Updates the lists in records as well as the database.
    """
    total_cols = _get_total_cols(records)

    # Update all columns if specific number not given
    if num_cols_to_update is None:
        num_cols_to_update = total_cols

    # Randomly choose columns to update
    update_idx = random.sample(range(1, total_cols), num_cols_to_update)

    update_time = 0.0
    timed_update = _timeit(query.update)

    print("Beginning update...")

    for key, record in records.items():
        update_vals = [None for _ in range(total_cols)]

        # Get random number to update to for each column
        for idx in update_idx:
            new_val = random.randint(*val_range)
            update_vals[idx] = new_val
            record[idx] = new_val

        _, t_diff = timed_update(key, *update_vals)
        update_time += t_diff

        # Check correctness
        new_record = query.select(key, 0, [1 for _ in range(total_cols)])[0]
        if new_record.columns != record:
            print(f"Update error: {new_record.columns} != {record}")
        
    print(f'Updating {len(records)} took {update_time}')
    print("Update finished")

    return update_time

def test_sum(
    query: Query,
    records: dict[int, list[int]], 
    start_range: int, # Start of key range
    end_range: int,   # End of key range
    agg_col_idx: int  # Index of column to aggregate
):
    sum_time = 0.0
    timed_sum = _timeit(query.sum)

    print("Beginning summation ...")

    for _ in records.values():
        _, t_diff = timed_sum(start_range, end_range, agg_col_idx)
        sum_time += t_diff
        
    print(f'Summing {len(records)} took {sum_time}')
    print("Sum finished")

    return sum_time

def test_delete(
    query: Query,
    records: dict[int, list[int]]
):
    total_cols = _get_total_cols(records)
    
    delete_time = 0.0

    timed_delete = _timeit(query.delete)

    print("Beginning deletion...")

    for key in records:
        _, t_diff = timed_delete(key)
        delete_time += t_diff

        del_record = query.select(key, 0, [1 for _ in range(total_cols)])

        if del_record:
            # Get first record in list
            del_record = del_record[0]
            
            print(f"Delete error: record {del_record.columns} still lives")
        
    print(f'Deleting {len(records)} took {delete_time}')
    print("Delete finished")

    return delete_time
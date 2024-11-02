from typing import Literal

import time
import random

from lstore.table import Record
from lstore.query import Query

def timeit(fn):
    def wrapper(*args, **kwargs):
        t0 = time.process_time()
        result = fn(*args, **kwargs)
        t1 = time.process_time()

        return result, t1 - t0
    return wrapper

def _get_total_cols(records):
    return len(next(iter(records.values())))

def gen_keys(num_records: int, shuffle=True):
    """Generates keys for the given number of records."""
    keys = list(range(num_records))
    key_range = (keys[0], keys[-1])
    
    if shuffle:
        random.shuffle(keys)

    return keys, key_range

def create_records(num_columns: int, num_records: int, val_range=(0, 20)):
    """Randomly creates records with the given specification (uniform sampling.)"""
    records = dict()

    keys, key_range = gen_keys(num_records)

    for key in keys:
        records[key] = [key] + [
            random.randint(*val_range) for _ in range(num_columns)
        ]

    return records, key_range

def test_insert(query: Query, records: dict[int, list[int]]):
    insert_time = 0.0

    timed_insert = timeit(query.insert)

    print("Beginning insertion...")

    for columns in records.values():
        _, t_diff = timed_insert(*columns)
        insert_time += t_diff
        
    print(f"Inserting {len(records):,} took {insert_time:.4f}s")
    print("Insert finished\n")

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
    timed_select = timeit(query.select)

    for key, record in records.items():
        result, t_diff = timed_select(key, 0, proj_col_idx)
        select_time += t_diff

        # Get record from returned list (point query)
        result: Record = result[0]

        # Check correctness
        j = 0 # Column index for projected result
        for i, include in enumerate(proj_col_idx):
            if not include:
                continue

            if result.columns[j] != record[i]:
                print(f'Error column {i + 1} does not match {records}')
            j += 1

    print(f"Selecting {len(records):,} took {select_time:.4f}s")
    print("Select finished\n")

    return select_time


def test_update_random(
    query: Query,
    records: dict[int, list[int]], 
    update_cols: int = None,
    gen_fn=random.uniform,
    gen_params=(0, 20)
):   
    """
    Randomly updates the given number of columns for each record.

    Does so according to the given number generation function and associated
    parameters (ex random.uniform(low, high), random.gauss(mean, stdev)).
    
    Updates the lists in records as well as the database values themselves.
    """
    total_cols = _get_total_cols(records)

    # Update all columns if specific number not given
    if update_cols is None:
        update_cols = total_cols

    update_time = 0.0
    timed_update = timeit(query.update)

    print("Beginning update...")

    for key, record in records.items():
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
        new_record = query.select(key, 0, [1 for _ in range(total_cols)])[0]
        if new_record.columns != record:
            print(f"Update error: {new_record.columns} != {record}")
        
    print(f"Updating {len(records):,} took {update_time:.4f}s")
    print("Update finished\n")

    return update_time

def test_sum(
    query: Query,
    records: dict[int, list[int]], 
    start_range: int, # Start of key range
    end_range: int,   # End of key range
    agg_col_idx: int  # Index of column to aggregate
):
    timed_sum = timeit(query.sum)

    print(f"Beginning summation for keys {start_range} to {end_range}...")

    result, t_diff = timed_sum(start_range, end_range, agg_col_idx)

    # Check correctness
    real_sum = 0.0
    for i in range(start_range, end_range + 1):
        real_sum += records[i][agg_col_idx]
    if result != real_sum:
        print(f"Sum error: sum({start_range}, {end_range})={real_sum} not {result}")
        
    print(f"Summing took {t_diff:.4f}s")
    print("Sum finished\n")

    return result, t_diff

def test_delete(
    query: Query,
    records: dict[int, list[int]]
):
    total_cols = _get_total_cols(records)
    
    delete_time = 0.0

    timed_delete = timeit(query.delete)

    print("Beginning deletion...")

    for key in records:
        _, t_diff = timed_delete(key)
        delete_time += t_diff

        del_record = query.select(key, 0, [1 for _ in range(total_cols)])

        # Correctness check
        if del_record:
            del_record = del_record[0]
            
            print(f"Delete error: record {del_record.columns} still lives")
        
    print(f"Deleting {len(records):,} took {delete_time:.4f}s")
    print("Delete finished\n")

    return delete_time

import time

import random

def timeit(fn):
    def wrapper(*args, **kwargs):
        t0 = time.process_time()
        result = fn(*args, **kwargs)
        t1 = time.process_time()

        return result, t1 - t0
    return wrapper

def get_total_cols(records: dict[int, list[int]]):
    return len(next(iter(records.values())))

def gen_keys(num_records: int, shuffle: bool = True):
    """Generates keys for the given number of records."""
    keys = list(range(num_records))
    key_range = (keys[0], keys[-1])
    
    if shuffle:
        random.shuffle(keys)

    return keys, key_range

def gen_records(num_records: int, num_columns: int, val_range: tuple[int, int],
                shuffle_records=True):
    keys, _ = gen_keys(num_records, shuffle_records)

    records = dict()
    for key in keys:
        records[key] = [key] +  [random.randint(*val_range) for _ in range(num_columns)]

    return records

import time
import random

def timeit(fn):
    def wrapper(*args, **kwargs):
        t0 = time.process_time()
        result = fn(*args, **kwargs)
        t1 = time.process_time()

        return result, t1 - t0
    return wrapper

def test_insert(query, keys, records):
    query.

    insert_time = 0.0

    timed_insert = timeit(query.insert)

    print("Beginning insertion...")
    for key in keys:
        records[key] = [key] + [random.randint(0, 20) for _ in range(num_columns)]
        
        _, t_diff = timed_insert(*records[key])
        insert_time += t_diff
    print(f'Inserting {number_of_records} took {insert_time}')
    print("Insert finished")

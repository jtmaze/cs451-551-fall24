from lstore.db import Database
from lstore.query import Query
from time import process_time

from random import choice, randint, sample, seed, shuffle

db = Database()
num_columns = 12
temp_table = db.create_table('MaxTemp', num_columns, 0)

query = Query(temp_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 200_000
number_of_aggregates = 100
seed(42)

def timeit(fn):
    def wrapper(*args, **kwargs):
        t0 = process_time()
        result = fn(*args, **kwargs)
        t1 = process_time()

        return result, t1 - t0
    return wrapper

# Insertion
keys = list(range(number_of_records))
shuffle(keys)
insert_time = 0.0
timed_insert = timeit(query.insert)
print("Beginning insertion...")
for key in keys:
    records[key] = [key] + [randint(0, 20) for _ in range(num_columns)]
    
    _, t_diff = timed_insert(*records[key])
    insert_time += t_diff
print(f'Inserting {number_of_records} took {insert_time}')
print("Insert finished")

# Select
for key in records:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)

updated_records = {}
for key in records:
    updated_columns = [None, None, None, None, None]
    updated_records[key] = records[key].copy()
    for i in range(2, temp_table.num_columns):
        # updated value
        value = randint(0, 20)
        updated_columns[i] = value
        # update our test directory
        updated_records[key][i] = value
    query.update(key, *updated_columns)

    #check version -1 for record
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
    error = False
    for j, column in enumerate(record.columns):
        if column != records[key][j]:
            error = True
    if error:
        print('update error on', records[key], 'and', updated_columns, ':', record, ', correct:', records[key])
    else:
        pass
        # print('update on', original, 'and', updated_columns, ':', record)

    #check version -2 for record
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0]
    error = False
    for j, column in enumerate(record.columns):
        if column != records[key][j]:
            error = True
    if error:
        print('update error on', records[key], 'and', updated_columns, ':', record, ', correct:', records[key])
    else:
        pass
        # print('update on', original, 'and', updated_columns, ':', record)
    
    #check version 0 for record
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0]
    error = False
    for j, column in enumerate(record.columns):
        if column != updated_records[key][j]:
            error = True
    if error:
        print('update error on', records[key], 'and', updated_columns, ':', record, ', correct:', updated_records[key])

keys = sorted(list(records.keys()))


# aggregate on every column 
for c in range(0, temp_table.num_columns):
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        # calculate the sum form test directory
        # version -1 sum
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum_version(keys[r[0]], keys[r[1]], c, -1)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        else:
            pass
            # print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
        # version -2 sum
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum_version(keys[r[0]], keys[r[1]], c, -2)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        else:
            pass
        # version 0 sum
        updated_column_sum = sum(map(lambda key: updated_records[key][c], keys[r[0]: r[1] + 1]))
        updated_result = query.sum_version(keys[r[0]], keys[r[1]], c, 0)
        if updated_column_sum != updated_result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', updated_result, ', correct: ', updated_column_sum)
        else:
            pass

if __name__ == "__main__":
    main()

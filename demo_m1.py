from time import process_time
from random import choice, randint, sample, seed, shuffle

from lstore.db import Database
from lstore.query import Query

import test_db

num_columns = 4
val_range = (-32, 100)

db = Database()
temp_table = db.create_table('MaxTemp', num_columns, 0)
query = Query(temp_table)

# dictionary for records to test the database: test directory
records = {}

num_records = 10
number_of_aggregates = 100
seed(42)

records, key_range = test_db.create_records(num_columns, num_records, val_range)

# Insertion
test_db.test_insert(query, records)

# Select
test_db.test_select(query, records, [1 for _ in range(num_columns)])

# Update
test_db.test_update(query, records, 6, val_range)

# Sum
test_db.test_sum(query, key_range[0], key_range[1])

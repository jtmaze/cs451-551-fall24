from time import process_time
from random import choice, randint, sample, seed, shuffle

from lstore.db import Database
from lstore.query import Query

import test_db

num_columns = 12        # Number of months
val_range = (-32, 100)  # Degrees F

# Create database with table having num_columns (+ 1 for primary key)
db = Database()
temp_table = db.create_table('MaxTemp', num_columns + 1, 0)
query = Query(temp_table)

# dictionary for records to test the database: test directory
records = {}

num_records = 10_000
number_of_aggregates = 100
seed(42)

records, key_range = test_db.create_records(num_columns, num_records, val_range)

# Insertion
test_db.test_insert(query, records)

# Select
test_db.test_select(query, records, None)

# Update (changes records too)
test_db.test_update_random(query, records, 6, val_range)

# Sum sweltering days in June (col_idx=5)
# test_db.test_sum(query, records, 70, 100, 5)

# Delete records
test_db.test_delete(query, records)

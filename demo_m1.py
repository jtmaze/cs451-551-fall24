import sys

from time import process_time
import random

from lstore.db import Database
from lstore.query import Query

import test_db

def generate_eugene_temps(num_records, st_dev=7):
    """Generates fictional records for monthly Eugene temperatures (F)"""
    # Eugene, OR temps from https://www.usclimatedata.com/climate/eugene/oregon/united-states/usor0118
    highs = [47, 51, 56, 61, 67, 73, 82, 83, 77, 64, 52, 46]
    lows = [34, 35, 37, 40, 44, 48, 51, 51, 47, 41, 38, 34]

    eugene_means = [(h + l) / 2 for h, l in zip(highs, lows)] 
    eugene_stdev = st_dev

    keys, key_range = test_db.gen_keys(num_records)
    records = dict()
    for key in keys:
        records[key] = [key] +  [int(random.gauss(mean, eugene_stdev)) for mean in eugene_means]

    return records

def a_series_of_horrific_and_unprecedented_natural_disasters_and_anomalous_weather_phenomena_strike_eugene_oregon(query, records, months=6):
    """Updates a given number of monthly temperatures per record to potentially extreme values."""
    test_db.test_update_random_uniform(query, records, update_cols=6, val_range=(-100, 200))

def PAUSE_BREAK(pauses):
    if pauses:
        input()

def main(pauses=False):
    num_columns = 12 # Number of months
    num_records = 100_000

    # Create database with table having num_columns (+ 1 for primary key)
    db = Database()
    temp_table = db.create_table('MaxTemp', num_columns + 1, 0)
    query = Query(temp_table)

    random.seed(42)

    records = generate_eugene_temps(num_records)
    print(f"\nExample record: {records[0]}\n")

    PAUSE_BREAK(pauses)

    # Insertion
    test_db.test_insert(query, records)

    PAUSE_BREAK(pauses)

    # Select
    test_db.test_select(query, records, None)

    PAUSE_BREAK(pauses)

    # Update (changes records too)
    print("WEATHER WARNING!!!")
    a_series_of_horrific_and_unprecedented_natural_disasters_and_anomalous_weather_phenomena_strike_eugene_oregon(query, records)
    print(f"New example record: {records[0]}\n")

    PAUSE_BREAK(pauses)

    # Sum sweltering days in June (col_idx=5)
    millenia_june_sum, _ = test_db.test_sum(query, records, 1000, 2000, 6)
    print(f"Sum of June temps in first decade: {millenia_june_sum:.2f}\n")

    PAUSE_BREAK(pauses)

    # Delete records
    test_db.test_delete(query, records)

if __name__ == "__main__":
    args = sys.argv[1:]

    if not args:
        main()
    else:
        main(pauses=True)

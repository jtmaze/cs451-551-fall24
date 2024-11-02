import sys

from time import process_time
import random

from lstore.db import Database
from lstore.query import Query

import test_db

# FOR DEMO ONLY, not required for database implementation
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def generate_eugene_temps(num_records, st_dev=7):
    """Generates fictional records for monthly Eugene temperatures (F)"""
    # Eugene, OR temps from https://www.usclimatedata.com/climate/eugene/oregon/united-states/usor0118
    highs = [47, 51, 56, 61, 67, 73, 82, 83, 77, 64, 52, 46]
    lows = [34, 35, 37, 40, 44, 48, 51, 51, 47, 41, 38, 34]

    eugene_means = [(h + l) / 2 for h, l in zip(highs, lows)] 
    eugene_stdev = st_dev

    keys, _ = test_db.gen_keys(num_records)
    records = dict()
    for key in keys:
        records[key] = [key] +  [int(random.gauss(mean, eugene_stdev)) for mean in eugene_means]

    return records

def a_series_of_horrific_and_unprecedented_yearly_natural_disasters_and_anomalous_weather_phenomena_strike_eugene_oregon(query, records, months=6):
    """Updates one monthly temperature per year  to potentially extreme values."""
    test_db.test_update_random(query, records, update_cols=1,
                               gen_fn=random.gauss, gen_params=(60, 50))

def pause_break(live, msg=""):
    if live:
        input(msg + " (press enter to continue) ")

def plot_temps(df, title):
    monthly_colors_hex = [
        "#0080FF", "#4B0082", "#90EE90", "#3CB371", "#FFDF00", "#FF69B4",
        "#FF4500", "#FFA500", "#DAA520", "#CD5C5C", "#8B4513", "#00BFFF"
    ]

    plt.figure(figsize=(10, 7))

    sns.stripplot(df.iloc[:, 1:], palette=monthly_colors_hex, size=2, alpha=0.4)

    plt.title(title)
    plt.xlabel("Month")
    plt.ylabel("Temp. (F)")
    plt.gca().set_axisbelow(True)
    plt.grid(axis="y", alpha=0.7)

    plt.show()

################################

def main(live=False):
    num_columns = 12 # Number of months
    num_records = 10_000

    # Create database with table having num_columns (+ 1 for primary key)
    db = Database()
    temp_table = db.create_table('MaxTemp', num_columns + 1, 0)
    query = Query(temp_table)

    random.seed(42)

    # ---------------------

    pause_break(live, "Generating temperature data...")

    records = generate_eugene_temps(num_records)

    if live:
        df = pd.DataFrame(records).T
        plot_temps(df, f"Eugene Temperatures for {num_records:,} Years")

    print(f"Example record: {records[0]}\n")

    # ---------------------

    pause_break(live, "Timing insertion of records into database...")

    # Insertion
    test_db.test_insert(query, records)

    # ---------------------

    pause_break(live, "Timing selection of all records...")

    # Select
    test_db.test_select(query, records, None)

    # ---------------------
    
    pause_break(live, "Updating all records for a random month...")

    # Update (changes records too)
    a_series_of_horrific_and_unprecedented_yearly_natural_disasters_and_anomalous_weather_phenomena_strike_eugene_oregon(query, records)
    print(f"New example record: {records[0]}\n")

    if live:
        df = pd.DataFrame(records).T
        plot_temps(df, f"Updated Eugene Temperature for {num_records:,} Years")

    # ---------------------

    pause_break(live, "Summing a millenia's worth of data...")

    # Sum sweltering days in June
    start_year = 1000
    end_year = 2000
    col_idx = 6 # June (primary key is 0)
    millenia_june_sum, _ = test_db.test_sum(query, records, start_year, end_year, col_idx)
    print(f"Sum of June temps in first decade: {millenia_june_sum:.2f}\n")

    # ---------------------
    
    pause_break(live, "Timing deletion of records from database...")

    # Delete records
    test_db.test_delete(query, records)

################################

if __name__ == "__main__":
    args = sys.argv[1:]

    if not args:
        main()
    else:
        main(live=True)

import sys
import os

# Add root dir to path to find lstore
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from time import process_time
import random

from lstore.db import Database
from lstore.query import Query

from lstore.index_types.index_config import IndexConfig
from lstore.index_types.bptree import BPTreeIndex
from lstore.index_types.dict_index import DictIndex

import demo.demo_utils as demo_utils

# FOR DEMO ONLY, not required for database implementation
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

    keys, _ = demo_utils.gen_keys(num_records)
    records = dict()
    for key in keys:
        records[key] = [key] +  [int(random.gauss(mean, eugene_stdev)) for mean in eugene_means]

    return records

def pause_break(live, msg=""):
    if live:
        input(msg + " (press enter to continue) ")

def plot_temps(df, title):
    monthly_colors_hex = [
        "#0080FF", "#4B0082", "#90EE90", "#3CB371", "#FFDF00", "#FF69B4",
        "#FF4500", "#FFA500", "#DAA520", "#CD5C5C", "#8B4513", "#00BFFF"
    ]

    plt.figure(figsize=(10, 7))

    sns.stripplot(df.iloc[:, 1:], palette=monthly_colors_hex, size=2, alpha=0.32)

    plt.title(title)
    plt.xlabel("Month")
    plt.ylabel("Temp. (F)")
    plt.gca().set_axisbelow(True)
    plt.grid(axis="y", alpha=0.7)

    plt.show()

################################

def main(live=False):
    num_columns = 12 # Number of months
    num_records = 50_000

    index_config = IndexConfig(DictIndex) # DictIndex or BPTreeIndex

    pause_break(live, f"Creating database with index of type {index_config.index_type}")

    # Create database with table having num_columns (+ 1 for primary key)
    db = Database()
    temp_table = db.create_table('MaxTemp', num_columns + 1, 0, index_config)
    query = Query(temp_table)

    random.seed(42)

    # ---------------------

    pause_break(live, "Generating temperature data...")

    records = generate_eugene_temps(num_records)

    if live:
        df = pd.DataFrame(records).T
        plot_temps(df, f"Monthly Eugene Temperatures for {num_records:,} Years")

    print(f"Example record: {records[0]}\n")

    # ---------------------

    pause_break(live, "Timing insertion of records into database...")

    demo_utils.demo_insert(query, records)

    # ---------------------

    pause_break(live, "Timing selection of all records...")

    demo_utils.demo_select(query, records, None)

    # ---------------------

    pause_break(live, "Timing selection of one full column: November...")

    demo_utils.demo_select(
        query, records, [0 if i != 11 else 1 for i in range(num_columns + 1)]
    )

    # ---------------------
    
    pause_break(live, "Updating records: YEARLY DEVASTATING AND UNPRECEDENTED WEATHER PHENOMENA HAVE HIT EUGENE OREGON...")

    demo_utils.demo_update_random(query, records, update_cols=1,
                               gen_fn=random.randint, gen_params=(-100, 200))
    
    print(f"New example record: {records[0]}\n")

    if live:
        df = pd.DataFrame(records).T
        plot_temps(df, f"Updated Monthly Eugene Temperature for {num_records:,} Years")

    # ---------------------

    pause_break(live, "Summing a millenia's worth of data...")

    start_year = 10_000
    end_year = 20_000
    col_idx = 6 # June (primary key is 0)
    decamillenia_june_sum, _ = demo_utils.demo_sum(query, records, start_year, end_year, col_idx)
    print(f"Sum of June temps in first decade: {decamillenia_june_sum:.2f}\n")

    # ---------------------
    
    pause_break(live, "Timing deletion of records from database...")

    # Delete records
    demo_utils.demo_delete(query, records)

################################

if __name__ == "__main__":
    args = sys.argv[1:]

    if not args:
        main()
    else:
        main(live=True)

from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed


def main():
    db = Database()
    db.open('./CS451')

    # Getting the existing Grades table
    grades_table = db.create_table('Grades', 5, 0)

    query = Query(grades_table)

    keys = []
    records = {}
    key_prefix = 92106429
    for i in range(5, 10):
        print(i)
        key = key_prefix + i
        keys.append(key)
        records[key] = [
            key, 
            randint(i * 20, (i + 1) * 20),
            randint(i * 20, (i + 1) * 20), 
            randint(i * 20, (i + 1) * 20), 
            randint(i * 20, (i + 1) * 20)
        ]

    for record in records:
        query.insert(record)


    """Easy to track these keys/values"""
    hit_prim_key1 = key_prefix + 1  # 92106430
    hit_prim_key2 = key_prefix + 2  # 92106431
    hit_prim_key3 = key_prefix + 3  # 92106432

    print("\nTracking Keys:")
    print(f"hit_prim_key1: {hit_prim_key1}")
    print(f"hit_prim_key2: {hit_prim_key2}")
    print(f"hit_prim_key3: {hit_prim_key3}")

    prior_record1 = [hit_prim_key1, 1, 2, 3, 4]
    prior_record2 = [hit_prim_key2, 100, 200, 300, 400]
    prior_record3 = [hit_prim_key3, 1000, 2000, 3000, 4000]
    
    for record in [prior_record1, prior_record2, prior_record3]:
        query.insert(*record)

    """
    First, test simultaneous updates on the same primary key
    """

    print("\n--- Test 1: Simultaneous Updates on the Same Primary Key ---")
    t1 = Transaction()
    t1.add_query(query.update, grades_table, hit_prim_key1, [None, 5, 5, 5, 5])
    t2 = Transaction()
    t2.add_query(query.update, grades_table, hit_prim_key1, [None, 10, 10, 10, 10])

    worker1 = TransactionWorker()
    worker1.add_transaction(t1)
    worker1.add_transaction(t2)

    worker1.run()
    worker1.join()

    result_after = query.select(hit_prim_key1, 0, [1,1,1,1,1])
    print(f'Most recent after both transactions', result_after)
    result_after_back1 = query.select(hit_prim_key1, -1, [1,1,1,1,1])
    print(f'Version -1 after both transactions', result_after_back1)

    print(f'T1 logs:', t1.logs, t1.delete_logs, t1.update_logs)
    print(f'T2 logs:', t2.logs, t2.delete_logs, t2.update_logs)

    """" 
    Next, test simultaneous delete and select opperations 
    based on primary key
    """

    print("\n--- Test 2: Simultaneous Delete and Select Operations ---")
    t3 = Transaction()
    t3.add_query(query.delete, grades_table, hit_prim_key1)
    t4 = Transaction()
    t4.add_query(query.select, grades_table, hit_prim_key1)

    worker2 = TransactionWorker()
    worker2.add_transaction(t3)
    worker2.add_transaction(t4)

    worker2.run()
    worker2.join()

    print(f'T3 logs:', t3.logs, t3.delete_logs, t3.update_logs)
    print(f'T4 logs:', t4.logs, t4.delete_logs, t4.update_logs)
    final_result = query.select(hit_prim_key1, 0, [1, 1, 1, 1, 1])
    print(f"\nFinal state of key {hit_prim_key1}: {final_result}")

    """ 
    Next test a secondary index select, while an update opperation is 
    happening (based on primary key)
    """
    print("\n--- Test 3: Update some ---")

    #TODO: finish this

    # t5 = Transaction()
    # t5.add_query(query.update, grades_table, hit_prim_key3, [None, 0, 0, 0, 0])
    # t6 = Transaction()
    # t6.add_query(query.select_version_range))


if __name__ == "__main__":
    main()

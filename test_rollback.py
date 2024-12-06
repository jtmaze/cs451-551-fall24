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
    for i in range(0, 10):
        key = 92106429 + i
        keys.append(key)
        records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]

    record = [0, 1, 2, 3, 4]

    t1 = Transaction()
    t2 = Transaction()
    worker1 = TransactionWorker()
    worker2 = TransactionWorker()

    t1.add_query(query.insert, grades_table, *record)
    t1.add_query(query.select, grades_table, record[0], 0, [1, 1, 1, 1, 1])
    
    t2.add_query(query.insert, grades_table, *record)
    t2.add_query(query.select, grades_table, record[0], 0, [1, 1, 1, 1, 1])

    worker1.add_transaction(t1)
    worker2.add_transaction(t2)

    for worker in (worker1, worker2):
        worker.run()
    
    for worker in (worker1, worker2):
        worker.join()


if __name__ == "__main__":
    main()

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

    transaction = Transaction()
    worker = TransactionWorker()

    transaction.add_query(query.insert, grades_table, *record)
    transaction.add_query(query.select, grades_table, record[0], 0, [1, 1, 1, 1, 1])
    transaction.add_query(query.insert, grades_table, *record)

    worker.add_transaction(transaction)

    worker.run()
    worker.join()

    print(query.select(0, 0, [1, 1, 1, 1, 1]))

    print(transaction.logs)
    print(transaction.delete_logs)
    print(transaction.update_logs)



if __name__ == "__main__":
    main()

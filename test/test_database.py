import unittest
from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
from random import randint, seed


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        self.db.open('./test_db')
        self.table = self.db.create_table('TestTable', 5, 0)

    def tearDown(self):
        self.db.close()

    # Test case: Basic table creation and metadata validation
    def test_table_creation(self):
        self.assertEqual(self.table.name, 'TestTable')
        self.assertEqual(self.table.num_columns, 5)
        self.assertEqual(self.table.key, 0)

    # Test case: Insert and select a single record
    def test_insert_and_select_single(self):
        query = Query(self.table)
        record = [1, 10, 20, 30, 40]
        query.insert(*record)
        selected_record = query.select(1, 0, [1, 1, 1, 1, 1])[0]
        self.assertEqual(selected_record.columns, record)

    # Test case: Edge case for inserting duplicate primary key
    def test_insert_duplicate_key(self):
        query = Query(self.table)
        record = [1, 10, 20, 30, 40]
        query.insert(*record)
        duplicate_record = [1, 50, 60, 70, 80]
        with self.assertRaises(Exception):  # Ensure exception is raised for duplicate keys
            query.insert(*duplicate_record)

    # Test case: Update a record
    def test_update(self):
        query = Query(self.table)
        record = [1, 10, 20, 30, 40]
        query.insert(*record)
        updated_values = [None, 15, None, 35, None]
        query.update(1, *updated_values)
        selected_record = query.select(1, 0, [1, 1, 1, 1, 1])[0]
        self.assertEqual(selected_record.columns, [1, 15, 20, 35, 40])

    # Test case: Delete a record and validate it no longer exists
    def test_delete(self):
        query = Query(self.table)
        record = [1, 10, 20, 30, 40]
        query.insert(*record)
        query.delete(1)
        self.assertFalse(query.select(1, 0, [1, 1, 1, 1, 1]))

    # Test case: Transaction commit
    def test_transaction_commit(self):
        query = Query(self.table)
        transaction = Transaction()
        record = [1, 10, 20, 30, 40]
        transaction.add_query(query.insert, self.table, *record)
        self.assertTrue(transaction.run())
        selected_record = query.select(1, 0, [1, 1, 1, 1, 1])[0]
        self.assertEqual(selected_record.columns, record)

    # Test case: Transaction abort with rollback
    def test_transaction_abort(self):
        query = Query(self.table)
        transaction = Transaction()
        record = [1, 10, 20, 30, 40]
        transaction.add_query(query.insert, self.table, *record)
        # Add invalid query to force abort
        transaction.add_query(query.update, self.table, 999, [None, 15, None, None, None])
        self.assertFalse(transaction.run())
        # Validate rollback (record should not exist)
        self.assertFalse(query.select(1, 0, [1, 1, 1, 1, 1]))

    # Test case: Concurrent transactions with TransactionWorker
    def test_transaction_worker(self):
        query = Query(self.table)
        transactions = []
        for i in range(10):
            transaction = Transaction()
            record = [i, randint(1, 100), randint(1, 100), randint(1, 100), randint(1, 100)]
            transaction.add_query(query.insert, self.table, *record)
            transactions.append(transaction)

        worker = TransactionWorker(transactions)
        worker.run()
        worker.join()
        self.assertEqual(worker.result, 10)  # Ensure all transactions committed

    # Test case: Sum operation
    def test_sum_operation(self):
        query = Query(self.table)
        records = [
            [1, 10, 20, 30, 40],
            [2, 15, 25, 35, 45],
            [3, 20, 30, 40, 50]
        ]
        for record in records:
            query.insert(*record)
        result = query.sum(1, 3, 1)  # Sum column 1 for primary keys 1 to 3
        self.assertEqual(result, 10 + 15 + 20)

    # Edge case: Empty table sum
    def test_sum_empty_table(self):
        query = Query(self.table)
        result = query.sum(1, 5, 1)
        self.assertEqual(result, 0)

    # Test case: Increment column value
    def test_increment(self):
        query = Query(self.table)
        record = [1, 10, 20, 30, 40]
        query.insert(*record)
        query.increment(1, 1)  # Increment column 1
        selected_record = query.select(1, 0, [1, 1, 1, 1, 1])[0]
        self.assertEqual(selected_record.columns, [1, 11, 20, 30, 40])

    # Edge case: Large number of records
    def test_large_number_of_records(self):
        query = Query(self.table)
        num_records = 1000
        records = {i: [i, randint(1, 100), randint(1, 100), randint(1, 100), randint(1, 100)] for i in range(1, num_records + 1)}
        for record in records.values():
            query.insert(*record)
        # Select all records and validate count
        selected_records = [query.select(i, 0, [1, 1, 1, 1, 1])[0] for i in range(1, num_records + 1)]
        self.assertEqual(len(selected_records), num_records)


if __name__ == '__main__':
    unittest.main()

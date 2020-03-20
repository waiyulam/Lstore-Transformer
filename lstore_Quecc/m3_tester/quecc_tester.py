import sys
sys.path.append(sys.path[0] + "/..")
from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
import threading
import os

class Simple_Tester:
# Setup: 2 transaction workers, 2 transactions / worker, 2 queries / transaction
    def __init__(self):
        if (os.path.isdir('ECS165')):
            os.system("rm -rf ECS165")
        self.db = Database()
        self.db.open('ECS165')
        self.table = self.db.create_table('Grades', 5, 0)
        self.keys = []
        self.records = {}
        self.num_threads = 2

    def setup(self):
        for i in range(8):
            key = 100 + i
            self.keys.append(key)
            record = [key, 1, 2, 3, 4]
            self.records[key] = record
            q = Query(self.table)
            q.insert(*self.records[key])

    def init_trans_workers(self):
        transaction_workers = []
        for i in range(self.num_threads):
            transaction_workers.append(TransactionWorker([], self.table, i))
        return transaction_workers

    def thread_run(self, trans_workers):
        threads = []
        for trans_worker in trans_workers:
            threads.append(threading.Thread(target = trans_worker.run, args = ()))
        for i, thread in enumerate(threads):
            print('Thread', i, 'started')
            thread.start()
            thread.join()
            print('Thread', i, 'finished')
        #for i, thread in enumerate(threads):



    '''
    Desired Output (order may be different but every one should appear exactly once):
    100, 1, 2, 3, 4
    101, 1, 2, 3, 4
    102, 1, 2, 3, 4
    103, 1, 2, 3, 4
    104, 1, 2, 3, 4
    105, 1, 2, 3, 4
    106, 1, 2, 3, 4
    107, 1, 2, 3, 4
    '''
    def simple_select(self):
        self.table.init_priority_queues(self.num_threads)
        transaction_workers = self.init_trans_workers()
        k = 0
        for i in range(2):
            for l in range(2):
                transaction = Transaction(i % self.num_threads)
                for j in range(2):
                    key = self.keys[k]
                    q = Query(self.table)
                    transaction.add_query(q.select, key, 0, [1, 1, 1, 1, 1])
                    k = k + 1
                transaction_workers[i % self.num_threads].add_transaction(transaction)
        # added all 8 queries into the two transaction workers
        self.thread_run(transaction_workers)

    '''
    Desired Output (order may be different but every one should appear exactly once):
    100, 0, 2, 3, 4
    101, 0, 2, 3, 4
    102, 0, 2, 3, 4
    103, 0, 2, 3, 4
    104, 0, 2, 3, 4
    105, 0, 2, 3, 4
    106, 0, 2, 3, 4
    107, 0, 2, 3, 4
    '''
    def simple_update(self):
        self.table.init_priority_queues(self.num_threads)
        transaction_workers = self.init_trans_workers()
        k = 0
        updated_columns = [None, 0, None, None, None]
        for i in range(2):
            for l in range(2):
                transaction = Transaction(i % self.num_threads)
                for j in range(2):
                    key = self.keys[k]
                    q = Query(self.table)
                    transaction.add_query(q.update, key, *updated_columns)
                    k = k + 1
                transaction_workers[i % self.num_threads].add_transaction(transaction)
        self.thread_run(transaction_workers)
        # self.simple_select()

    def simple_sum(self):
        self.table.init_priority_queues(self.num_threads)
        transaction_workers = self.init_trans_workers()
        q = Query(self.table)

        transaction = Transaction(0)
        transaction.add_query(q.sum, self.keys[0], self.keys[3], 0) # 406
        transaction.add_query(q.sum, self.keys[0], self.keys[3], 2) # 8
        transaction_workers[0].add_transaction(transaction)

        transaction = Transaction(0)
        transaction.add_query(q.sum, self.keys[0], self.keys[3], 3) # 12
        transaction.add_query(q.sum, self.keys[0], self.keys[3], 4) # 16
        transaction_workers[0].add_transaction(transaction)

        transaction = Transaction(1)
        transaction.add_query(q.sum, self.keys[4], self.keys[7], 0) # 422
        transaction.add_query(q.sum, self.keys[4], self.keys[7], 2) # 8
        transaction_workers[1].add_transaction(transaction)

        transaction = Transaction(1)
        transaction.add_query(q.sum, self.keys[4], self.keys[7], 3) # 12
        transaction.add_query(q.sum, self.keys[4], self.keys[7], 4) # 16
        transaction_workers[1].add_transaction(transaction)

        self.thread_run(transaction_workers)

    def simple_delete(self):
        self.table.init_priority_queues(self.num_threads)
        transaction_workers = self.init_trans_workers()
        k = 0
        for i in range(2):
            for l in range(2):
                transaction = Transaction(i % self.num_threads)
                for j in range(2):
                    key = self.keys[k]
                    q = Query(self.table)
                    transaction.add_query(q.delete, key)
                    k = k + 1
                transaction_workers[i % self.num_threads].add_transaction(transaction)
        self.thread_run(transaction_workers)
        self.simple_select()

    def run_all(self):
        self.setup()
        self.simple_select()
        # self.simple_update()
        # self.simple_sum()
        # self.simple_delete()
        os.system('rm -rf ECS165')

class One_Thread_Tester:
# 1 transaction worker, 1 transaction, 8 quries
    def __init__(self):
        if (os.path.isdir('ECS165')):
            os.system("rm -rf ECS165")
        self.db = Database()
        self.db.open('ECS165')
        self.table = self.db.create_table('Grades', 5, 0)
        self.keys = []
        self.records = {}
        self.num_records = 8

    def setup(self):
        for i in range(self.num_records):
            key = 100 + i
            self.keys.append(key)
            record = [key, 1, 2, 3, 4]
            self.records[key] = record
            q = Query(self.table)
            q.insert(*self.records[key])

    def init_trans_worker(self):
        self.table.init_priority_queues(1)
        transaction_worker = TransactionWorker([], self.table, 0)
        transaction = Transaction(0)
        q = Query(self.table)
        return q, transaction, transaction_worker

    def one_select(self):
        q, transaction, transaction_worker = self.init_trans_worker()
        for i in range(self.num_records):
            transaction.add_query(q.select, self.keys[i], 0, [1, 1, 1, 1, 1])
        transaction_worker.add_transaction(transaction)
        transaction_worker.run()

    def one_update(self):
        q, transaction, transaction_worker = self.init_trans_worker()
        updated_columns = [None, 0, None, None, None]
        for i in range(self.num_records):
            transaction.add_query(q.update, self.keys[i], *updated_columns)
        transaction_worker.add_transaction(transaction)
        transaction_worker.run()
        self.one_select()

    def one_sum(self):
        # sum all five columns
        # sample output: 828, 0, 16, 24, 32
        q, transaction, transaction_worker = self.init_trans_worker()
        for i in range(5):
            transaction.add_query(q.sum, self.keys[0], self.keys[self.num_records - 1], i)
        transaction_worker.add_transaction(transaction)
        transaction_worker.run()

    def one_delete(self):
        q, transaction, transaction_worker = self.init_trans_worker()
        for i in range(self.num_records):
            transaction.add_query(q.delete, self.keys[i])
        transaction_worker.add_transaction(transaction)
        transaction_worker.run()
        self.one_select() # should print all DELETED value = 2**64-2

    def run_all(self):
        self.setup()
        self.one_select()
        # self.one_update()
        # self.one_sum()
        # self.one_delete()
        os.system('rm -rf ECS165')

def main():
    print("\n*** One Thread Tester ***\n")
    one_thread_tester = One_Thread_Tester()
    one_thread_tester.run_all()
    # print("\n*** Simple Quecc Tester ***\n")
    # simple_tester = Simple_Tester()
    # simple_tester.run_all()

if __name__ == "__main__":
    os.system("clear")
    main()

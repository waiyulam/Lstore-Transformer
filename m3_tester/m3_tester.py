import sys
sys.path.append(sys.path[0] + "/..")
from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
import threading
from random import choice, randint, sample, seed

import os
os.system("clear")
if (os.path.isdir('ECS165')):
    os.system("rm -rf ECS165")

db = Database()
db.open('ECS165')
grades_table = db.create_table('Grades', 5, 0)

keys = []
records = {}
seed(3562901)
num_threads = 8

# Generate random records
for i in range(0, 10000):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, 0, 0, 0, 0]
    q = Query(grades_table)
    q.insert(*records[key])

# Create transactions and assign them to workers
transactions = []
transaction_workers = []
for i in range(num_threads):
    transaction_workers.append(TransactionWorker())

for i in range(10000):
    key = choice(keys)
    record = records[key]
    c = record[1]
    transaction = Transaction()
    for i in range(5):
        c += 1
        q = Query(grades_table)
        transaction.add_query(q.select, key, 0, [1, 1, 1, 1, 1]) 
        q = Query(grades_table)
        transaction.add_query(q.update, key, *[None, c, None, None, None])
    transaction_workers[i % num_threads].add_transaction(transaction)

threads = []
for transaction_worker in transaction_workers:
    threads.append(threading.Thread(transaction_worker.run, args = ()))

for thread in threads:
    thread.start()

for thread in threads:
    thread.wait()

num_committed_transactions = sum(t.result for t in transaction_workers)

db.close()

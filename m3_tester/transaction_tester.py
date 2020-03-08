from template.db import Database
from template.query import Query
from template.transaction import Transaction
from template.transaction_worker import TransactionWorker

import threading
from random import choice, randint, sample, seed

db = Database()
db.open('/home/pkhorsand/165a-winter-2020-private/db')
grades_table = db.create_table('Grades', 5, 0)

keys = []
records = {}
num_threads = 8
seed(8739878934)

# Generate random records
for i in range(0, 10000):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, 0, 0, 0, 0]
    q = Query(grades_table)
    q.insert(*records[key])

# create TransactionWorkers
transaction_workers = []
for i in range(num_threads):
    transaction_workers.append(TransactionWorker([]))

# generates 10k random transactions
# each transaction will increment the first column of a record 5 times
for i in range(1000):
    k = randint(0, 2000 - 1)
    transaction = Transaction()
    for j in range(5):
        key = keys[k * 5 + j]
        q = Query(grades_table)
        transaction.add_query(q.select, key, 0, [1, 1, 1, 1, 1])
        q = Query(grades_table)
        transaction.add_query(q.increment, key, 1)
    transaction_workers[i % num_threads].add_transaction(transaction)

threads = []
for transaction_worker in transaction_workers:
    threads.append(threading.Thread(target = transaction_worker.run, args = ()))

for i, thread in enumerate(threads):
    print('Thread', i, 'started')
    thread.start()

for i, thread in enumerate(threads):
    thread.join()
    print('Thread', i, 'finished')

num_committed_transactions = sum(t.result for t in transaction_workers)
print(num_committed_transactions, 'transaction committed.')

query = Query(grades_table)
s = query.sum(keys[0], keys[-1], 1)

if s != num_committed_transactions * 5:
    print('Expected sum:', num_committed_transactions * 5, ', actual:', s, '. Failed.')
else:
    print('Pass.')

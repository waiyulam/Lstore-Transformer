import sys
sys.path.append(sys.path[0] + "/../..")
from lstore.db_quecc import Database
from lstore.query_quecc import Query
from lstore.transaction_quecc import Transaction
from lstore.transaction_worker_quecc import TransactionWorker
import threading
from random import choice, randint, sample, seed
from time import process_time
import os
os.system("clear")

if (os.path.isdir('ECS165')):
    os.system("rm -rf ECS165")
# Student Id and 4 grades
db = Database()
db.open('ECS165')
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
grades_table.init_priority_queues(num_threads)
for i in range(num_threads):
    transaction_workers.append(TransactionWorker([], grades_table, i))

# generates 10k random queries
# each transaction will increment the first column of a record 5 times
contention = 2000 # change contention here
for i in range(1000):
    k = randint(0, contention - 1)
    transaction = Transaction(i % num_threads)
    for j in range(5):
        key = keys[k * 5 + j]
        q = Query(grades_table)
        transaction.add_query(q.select, key, 0, [1, 1, 1, 1, 1])
        q = Query(grades_table)
        updated_columns = [None, 0, None, None, None]
        transaction.add_query(q.update, key, *updated_columns)
    transaction_workers[i % num_threads].add_transaction(transaction)

threads = []
for transaction_worker in transaction_workers:
    threads.append(threading.Thread(target = transaction_worker.run, args = ()))

time0 = process_time()
for i, thread in enumerate(threads):
    # print('Thread', i, 'started')
    thread.start()
    thread.join()
    # print('Thread', i, 'finished')
    
time1 = process_time()
time_elapse = time1 - time0
# print("10K Queries took:  \t\t\t", time_elapse)

print("time: \t\t\t", time_elapse)
print('Contention:', (2000 - contention) / 2000)
print('Throughput:', 10000 / time_elapse)

os.system("rm -rf ECS165")
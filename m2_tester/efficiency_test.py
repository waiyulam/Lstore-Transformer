import sys
sys.path.append(sys.path[0] + "/..")
from lstore.db import Database
from lstore.query import Query
from random import choice, randint, sample, seed
from time import process_time
import os
os.system("clear")

# Student Id and 4 grades
db = Database()
db.open('ECS165')

# First Time => create_table, afterwards => get_table
# if want to keep the same base and tail, comment INSERT and UPDATE tests
grades_table = db.create_table('Grades', 5, 0)
# grades_table = db.get_table('Grades')

query = Query(grades_table)
records = {}
seed(3562901)

# Measuring Insert Performance
insert_time_0 = process_time()
for i in range(0, 1000):
    key = 92106429 + i # distinct primary key
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
insert_time_1 = process_time()
print("Inserting 1k records took:  \t\t\t", insert_time_1 - insert_time_0)

# Measuring Select Performance
select_time_0 = process_time()
for key in records:
    query.select(key, 0, [1, 1, 1, 1, 1])[0]
select_time_1 = process_time()
print("Selecting 1k records took:  \t\t\t", select_time_1 - select_time_0)

# Measuring update Performance
update_time_0 = process_time()
for key in records:
    updated_columns = [None, None, None, None, None]
    for i in range(1, grades_table.num_columns):
        value = randint(0, 20)
        updated_columns[i] = value
        query.update(key, *updated_columns)
update_time_1 = process_time()
print("Updating 1k records of total 4000 times took:  \t\t\t", update_time_1 - update_time_0)

# Measuring Aggregate Performance
num_batch = 100
batch_size = 100
keys = sorted(list(records.keys()))
agg_time_0 = process_time()
for c in range(0, grades_table.num_columns):
    for i in range(0, num_batch):
        r0 = sample(range(0, len(keys) - batch_size), 1)
        r = [r0[0], r0[0] + batch_size]
        result = query.sum(keys[r[0]], keys[r[1]], c)
agg_time_1 = process_time()
print("Aggregate 100 of 100 record batch for each column took:\t", agg_time_1 - agg_time_0)

# Measuring Delete Performance
delete_time_0 = process_time()
keys = sorted(list(records.keys()))
for mykey in keys:
    query.delete(mykey)
delete_time_1 = process_time()
print("Deleting 1k records took:  \t\t\t", delete_time_1 - delete_time_0)
db.close()

os.system("rm -rf ECS165")

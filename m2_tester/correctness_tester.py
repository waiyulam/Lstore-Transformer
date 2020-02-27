import sys
sys.path.append(sys.path[0] + "/..")
from lstore.db import Database
from lstore.query import Query
from lstore.config import *
from random import choice, randint, sample, seed
import os
os.system("clear")


# Student Id and 4 grades
db = Database()
db.open('m2_tester/ECS165')

# First Time => create_table, afterwards => get_table
# if want to keep the same base and tail, comment INSERT and UPDATE tests
grades_table = db.create_table('Grades', 5, 0)
# grades_table = db.get_table('Grades')

query = Query(grades_table)
records = {}
seed(3562901)

'''
INSERT 1000 records
'''
for i in range(0, 1000):
    key = 92106429 + randint(0, 9000)
    while key in records:
        key = 92106429 + randint(0, 9000)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    # print('inserted', records[key])
print("INSERT done.")

'''
SELECT TEST
'''
for key in records:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    # only select the primary key so it is a unique record
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key , ':', record, ', correct:', records[key])
        exit()
    # else:
    #     print('select on', key, ':', record)
print('Passed SELECT test.')

'''
UPDATE TEST: 
for each record, there will be 4 updates, so in total there should be
1000 * 4 = 40000 updates
'''
for key in records:
    updated_columns = [None, None, None, None, None]
    for i in range(1, grades_table.num_columns):
        value = randint(0, 20)
        updated_columns[i] = value
        original = records[key].copy()
        records[key][i] = value
        query.update(key, *updated_columns)
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        # only select the primary key so it is a unique record
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
            exit()
        # else:
        #     print('update on', original, 'and', updated_columns, ':', record)
        updated_columns[i] = None
print('Passed UPDATE test.')

'''
SUM TEST: 
Aggregate 100 of 100 record batch for each column
'''
num_batch = 100
batch_size = 100
keys = sorted(list(records.keys()))
for c in range(0, grades_table.num_columns):
    for i in range(0, num_batch):
        r0 = sample(range(0, len(keys) - batch_size), 1)
        r = [r0[0], r0[0] + batch_size]
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum(keys[r[0]], keys[r[1]], c)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
            exit()
        # else:
        #     print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
print('Passed SUM test.')

'''
DELETE TEST
'''
for key in records:
    query.delete(key)
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != MAXINT:
            error = True
    if error:
        print("record: " + str(key) + "does not delete completely")
print('Passed DELETE test.')
db.close()

os.system("rm -rf m2_tester/ECS165")
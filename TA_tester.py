from template.db import Database
from template.query import Query
from template.config import *

from random import choice, randint, sample
# from colorama import Fore, Back, Style
from time import process_time
import os
os.system("clear")

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}

# Measuring Insert Performance
insert_time_0 = process_time()
for i in range(0, 1000):
    key = 92106429 + randint(0, 9000)
    while key in records:
        key = 92106429 + randint(0, 9000)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
insert_time_1 = process_time()
print("Inserting 1k records took:  \t\t\t", insert_time_1 - insert_time_0)

# Measuring Select Performance
select_time_0 = process_time()
for key in records:
    record = query.select(key, [1, 1, 1, 1, 1])[0]
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            print(Fore.RED + 'Select error on key', key)
            exit()
# print(Fore.GREEN + 'Passed SELECT test.')
select_time_1 = process_time()
print("Selecting 1k records took:  \t\t\t", select_time_1 - select_time_0)

# Measuring update Performance
update_time_0 = process_time()
for key in records:
    updated_columns = [None, None, None, None, None]
    for i in range(1, grades_table.num_columns):
        value = randint(0, 20)
        updated_columns[i] = value
        records[key][i] = value
        record = query.select(key, [1, 1, 1, 1, 1])[0]
        query.update(key, *updated_columns)
        record = query.select(key, [1, 1, 1, 1, 1])[0]
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                print(Fore.RED + 'Update error for key', key)
                print(Fore.RED + 'Should have been', updated_columns)
                print(Fore.RED + 'Returned result:', record)
                exit()
        updated_columns[i] = None
# print(Fore.GREEN + 'Passed UPDATE test.')
update_time_1 = process_time()
print("Updating 1k records took:  \t\t\t", update_time_1 - update_time_0)

# Measuring Aggregate Performance
agg_time_0 = process_time()
keys = sorted(list(records.keys()))
for c in range(0, grades_table.num_columns):
    for i in range(0, 20):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum(keys[r[0]], keys[r[1]], c)
        if column_sum != result:
            print(Fore.RED + 'Sum error for keys', keys[r[0]], 'to', keys[r[1]], 'on column', c)
            print(Fore.RED + 'Should have been', column_sum, ' but returned ', result)
            exit()
# print(Fore.GREEN + 'Passed SUM test.')
agg_time_1 = process_time()
print("Aggregate 1k of 100 record batch took:\t", agg_time_1 - agg_time_0)

# Measuring Delete Performance
delete_time_0 = process_time()
for i in range(0, 1000):
    query.delete(906659671 + i)
delete_time_1 = process_time()
print("Deleting 1k records took:  \t\t\t", delete_time_1 - delete_time_0)

from lstore.db import Database
from lstore.query import Query
from lstore.config import *
from time import process_time

import os
os.system("clear")

# Student Id and 4 grades
db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 2, 0)
# grades_table = db.get_table('Grades')
query = Query(grades_table)
records = {}

'''
create only one record and update the same record
for multiple times (for examples, 1200 > 2 * 512 = 1024)
to trigger merge
'''

key = 914285714
# records[key] = [key, 123456]
# query.insert(*records[key])
# print("Insert one record")

for i in range(2000):
    record = query.select(key, 0, [1, 1])[0]

# update_time_0 = process_time()
# t = 1
# for i in range(1025):
#     updated_columns = [None, t]
#     query.update(key, *updated_columns)
#     t += 1
# update_time_1 = process_time()
# print("Updating one record for " + str(t - 1) + " times took:  \t\t\t", update_time_1 - update_time_0)

db.close()
# os.system("rm -rf ECS165")
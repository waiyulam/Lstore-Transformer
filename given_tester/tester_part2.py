from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

# Student Id and 4 grades
db = Database()
db.open('~/ECS165')
grades_table = db.get_table('Grades')
query = Query(grades_table)

# repopulate with random data
records = {}
seed(3562901)
for i in range(0, 1000):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
keys = sorted(list(records.keys()))
for _ in range(10):
    for key in keys:
        for j in range(1, grades_table.num_columns):
            value = randint(0, 20)
            records[key][j] = value
keys = sorted(list(records.keys()))
for key in keys:
    print(records[key])
    print(records[key])

for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
print("Select finished")

deleted_keys = sample(keys, 100)
for key in deleted_keys:
    query.delete(key)
    records.pop(key, None)

for i in range(0, 100):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
    result = query.sum(keys[r[0]], keys[r[1]], 0)
    if column_sum != result:
        print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
print("Aggregate finished")

db.close()

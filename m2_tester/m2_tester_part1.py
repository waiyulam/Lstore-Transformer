from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open('~/ECS165')
# Student Id and 4 grades
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}
seed(3562901)
for i in range(0, 1000):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
keys = sorted(list(records.keys()))
print("Insert finished")

table.index.create_index(1)
table.index.create_index(2)
table.index.create_index(3)
table.index.create_index(4)

for c in range(self.num_columns):
    _keys = list(set(record[c] for record in records))
    index = {v: [record for record in records if record[c] == v] for v in _keys}
    for key in _keys:
        results = query.select(key, c, [1, 1, 1, 1, 1])
        error = False
        if len(results) != len(index[key]):
            error = True
        if not error:
            for record in index[key]:
                if record not in results:
                    error = True
                    break
        if error:
            print('select error on', key, ', column', c ':', results, ', correct:', index[key])
print("Select finished")

for _ in range(10):
    for key in keys:
        updated_columns = [None, None, None, None, None]
        for i in range(1, grades_table.num_columns):
            value = randint(0, 20)
            updated_columns[i] = value
            original = records[key].copy()
            records[key][i] = value
            query.update(key, *updated_columns)
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            error = False
            for j, column in enumerate(record.columns):
                if column != records[key][j]:
                    error = True
            if error:
                print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
            # else:
            #     print('update on', original, 'and', updated_columns, ':', record)
            updated_columns[i] = None
print("Update finished")

for i in range(0, 100):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
    result = query.sum(keys[r[0]], keys[r[1]], 0)
    if column_sum != result:
        print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
    # else:
    #     print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
print("Aggregate finished")
db.close()

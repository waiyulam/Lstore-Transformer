import sys
sys.path.append(sys.path[0] + "/..")
from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open('ECS165')
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

grades_table.index.create_index(1)
grades_table.index.create_index(2)
grades_table.index.create_index(3)
grades_table.index.create_index(4)

_records = [records[key] for key in keys]
for c in range(grades_table.num_columns):
    _keys = list(set([record[c] for record in _records]))
    index = {v: [record for record in _records if record[c] == v] for v in _keys}
    for key in _keys:
        results = [r.columns for r in query.select(key, c, [1, 1, 1, 1, 1])]
        error = False
        if len(results) != len(index[key]):
            error = True
        if not error:
            for record in index[key]:
                if record not in results:
                    error = True
                    break
        if error:
            print('select error on', key, ', column', c, ':', results, ', correct:', index[key])
print("Select finished")

db.close()
exit()

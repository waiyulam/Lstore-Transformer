from template.db import Database
from template.query import Query
from template.config import init

from random import choice, randint, sample
from colorama import Fore, Back, Style

# Student Id and 4 grades
init()
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}

for i in range(0, 1000):
    key = 92106429 + randint(0, 9000)
    while key in records:
        key = 92106429 + randint(0, 9000)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])

for key in records:
    record = query.select(key, [1, 1, 1, 1, 1])[0]
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            print(Fore.RED + 'Select error on key', key)
            exit()
print(Fore.GREEN + 'Passed SELECT test.')

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
print(Fore.GREEN + 'Passed UPDATE test.')

keys = sorted(list(records.keys()))
for c in range(0, grades_table.num_columns):
    for i in range(0, 20):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum(keys[r[0]], keys[r[1]], c)
        if column_sum != result:
            print(Fore.RED + 'Sum error for keys', keys[r[0]], 'to', keys[r[1]], 'on column', c)
            print(Fore.RED + 'Should have been', column_sum, ' but returned ', result)

print(Fore.GREEN + 'Passed SUM test.')





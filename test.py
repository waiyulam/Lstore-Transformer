import os
from template.page import Page
from template.table import Table
from template.query import Query
from template.db import Database


class Query_Tester:
    def __init__(self):
        self.db = Database()
        self.table = self.db.create_table("test", 6, 0)
        self.query = Query(self.table)

    def test_insert(self):
        keys = []
        for i in range(0, 10):
            self.query.insert(906659671 + i, 93, i, i, i, i )
            keys.append(906659671 + i)
            page_index,record_index = self.table.get(keys[i])
            records = []
            for j in range(len(self.table.page_directory["Base"])):
                byte_value = self.table.page_directory["Base"][j][page_index].get(record_index)
                if (j == 1):
                    records.append(byte_value.decode()[4:])
                else:
                    records.append(int.from_bytes(byte_value, byteorder='big'))
            print(records)

    # select record without updates 
    def test_select1(self):
        res = self.query.select(906659671, [1, 1, 1, 1, 1, 1])
        print(res)
        res = self.query.select(906659681, [1, 0, 1, 0, 1, 1])
        print(res)

    def test_update1(self):
        self.query.update(906659671, *[None, 94, None, None, None, None])
        res = self.query.select(906659671, [1, 1, 1, 1, 1, 1])
        print(res)


    def test_sum(self):
        for i in range(0, 5):
            self.query.insert(906659671 + i, 93, 0, 0, 0, 0 )


    def run_all(self):
        self.test_insert()
        self.test_select1()
        self.test_update1()

def main():
    query_tester = Query_Tester()
    query_tester.run_all()

if __name__ == "__main__":
    main()

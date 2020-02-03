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

class Table_Tester:
    def __init__(self):
        self.db = Database()
        self.table = self.db.create_table('Grades', 5, 0)
        self.query = Query(self.table)
        self.keys = []
        for i in range(10):
            self.query.insert(100 + i, 93, 65, 43, 87)
            self.keys.append(100 + i)
    
    def get_tester(self):
        key = 103
        page_index,record_index = self.table.get(key)
        print("page_index = " + str(page_index) + 
        " record_index = " + str(record_index))
    
    def ktr_tester(self):
        key = 103
        rid = self.table.key_to_rid(key)
        rid = rid.decode("utf-8") 
        print("RID = " + str(rid))

    def itk_tester(self):
        index = 3
        key = self.table.index_to_key(index)
        key = int.from_bytes(key, byteorder = "big")
        print("KEY = " + str(key))

    def sum_tester(self):
        result = self.query.sum(0, 3, 2)
        print("score: = " + str(result))

    def run_all(self):
        self.get_tester()
        self.ktr_tester()
        self.itk_tester()

        # put here for now, should be in query
        self.sum_tester()

def main():
    print("\n*** TEST query ***\n")
    query_tester = Query_Tester()
    query_tester.run_all()
    print("\n*** TEST table ***\n")
    table_tester = Table_Tester()
    table_tester.run_all()

if __name__ == "__main__":
    os.system("clear")
    main()

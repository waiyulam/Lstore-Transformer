import os
from template.page import Page
from template.table import Table
from template.query import Query
from template.db import Database
from template.index import Index
from random import choice, randrange


class Query_Tester:
    def __init__(self):
        self.db = Database()
        self.table = self.db.create_table("test", 6, 0)
        self.query = Query(self.table)

    def test_delete(self):
        self.query.delete(906659671)
        keys = [906659671, 906659672, 906659673, 906659674, 906659675, 906659676, 906659677, 906659678, 906659679, 906659680]
        for i in range(0, 10):
            page_index,record_index = self.table.get(self.table.keys[i])
            records = []
            for j in range(len(self.table.page_directory["Base"])):
                byte_value = self.table.page_directory["Base"][j][page_index].get(record_index)
                if (j == 1):
                    records.append(byte_value.decode())
                else:
                    records.append(int.from_bytes(byte_value, byteorder='big'))
            print(records)

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
                    records.append(byte_value.decode())
                else:
                    records.append(int.from_bytes(byte_value, byteorder='big'))
            print(records)

    # select record without updates
    def test_select1(self):
        print("test: select\n")
        res = self.query.select(906659671, [1, 1, 1, 1, 1, 1])
        print(res)
        res = self.query.select(906659681, [1, 0, 1, 0, 1, 1])
        print(res)

    def test_update1(self):
        print("test: update\n")
        self.query.update(906659671, *[None, 94, None, None, None, None])
        res = self.query.select(906659671, [1, 1, 1, 1, 1, 1])
        print(res)
        self.query.update(906659671, *[None, 97, None, None, None, None])
        res = self.query.select(906659671, [1, 1, 1, 1, 1, 1])
        print(res)
        self.query.update(906659671, *[None, None, None, 92, None, None])
        res = self.query.select(906659671, [1, 1, 1, 1, 1, 1])
        print(res)
        # for i in range(len(self.table.page_directory["Tail"][0])):
        #     for j in range(0,10):
        #         byte_value = self.table.page_directory["Tail"][0][i].get(j)
        #         print(byte_value.decode())


    def test_sum(self):
        for i in range(0, 5):
            self.query.insert(906659671 + i, 93, 0, 0, 0, 0 )


    def run_all(self):
        self.test_insert()
        self.test_select1()
        self.test_update1()
        self.test_select1()
        self.test_delete()


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

    def table_column(self):
        key = 1
        column = self.table.get_column(key, 3)
        print("the whole RID column:")
        print(column)

    def table_column_update(self):
        self.query.update(101, *[None, 97, None, None, None])
        self.query.update(104, *[None, None, 100, 100, None])
        for i in range(0, 5):
            column, additional_column = self.table.get_column(i, 1)
            print("the updated column with rids ", i, " is: ")
            print(column)
            print("corresponding rid: ")
            print(additional_column)


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
        self.table_column()

        self.table_column()
        self.table_column_update()
        # put here for now, should be in query
        # self.sum_tester()

class Index_Tester:
    def __init__(self):
        #  print individual tree columns
        self.db = Database()
        self.table = self.db.create_table('Grades', 5, 0)
        self.query = Query(self.table)
        self.keys = []
        list = [0,1,2,3,4,5,6,7,8,9]
        for i in range(10):
            temp = choice(list)
            self.query.insert(temp+100, 93, 65, 43, 87)
            self.keys.append(temp+100)
            list.remove(temp)
        self.index = Index(self.table)

    def check_tree_structure(self):
        indices = self.index.indices
        # print out each tree information, corresponding to each column information
        print(indices)
        for i, indice in enumerate(indices):
            print("column", i)
            col_keys = indice.keys()
            for key in col_keys:
                print("key: ", key)
                # exlusively check the current value corresponding to how many record ID
                print(list(indice.values(min=key, max = key+1, excludemax=True)))

    def check_tree_locate(self):
        column_selected = 2
        print("record respect to 65")
        print(self.index.locate(column_selected, 65))
        print("record respect to 43(supposed to be None)")
        print(self.index.locate(column_selected, 43))

    def check_tree_locate_range(self):
        column_selected = 0
        print("record respect to 100 to 102")
        print(self.index.locate_range(100, 102, 0))
        print("record respect to 103 to 109")
        print(self.index.locate_range(103, 200, 0))
        print("record respect to 108 to 205")
        print(self.index.locate_range(108, 205, 0))
        print("record respect to 205 to 108")
        print(self.index.locate_range(205, 108, 0))

    def run_all(self):
        self.check_tree_structure()
        self.check_tree_locate()
        self.check_tree_locate_range()

def main():
    print("\n*** TEST query ***\n")
    query_tester = Query_Tester()
    query_tester.run_all()
#    print("\n*** TEST table ***\n")
#    table_tester = Table_Tester()
#    table_tester.run_all()
    #print("\n*** TEST index ***\n")
    #index_tester =  Index_Tester()
    #index_tester.run_all()

if __name__ == "__main__":
    os.system("clear")
    main()

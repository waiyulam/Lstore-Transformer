import os
from template.page import Page
from template.table import Table
from template.query import Query
from template.db import Database


class Query_Tester:
    def __init__(self):
        self.db = Database()
        self.table = self.db.create_table("test", 6, 0)

    def test_insert(self):
        self.query = Query(self.table)

        for i in range(514):
            self.query.insert(i, 1+i, 2+i, 3+i, 4+i, 5+i)

        # for col_id in range(len(self.table.page_directory["Base"])):
        #     for p_val in self.table.page_directory["Base"][col_id]:
        #         print("{}".format(col_id))
        #         print(p_val.data[0:100])


    def test_select(self):
        # for i in range(512):
        res = self.query.select(512, [1, 1, 1, 1, 1, 1])
        print(res)

        res = self.query.select(512, [0, 1, 0, 1, 0, 1])
        print(res)

        res = self.query.select(511, [1, 1, 1, 1, 1, 1])
        print(res)

        res = self.query.select(510, [1, 0, 1, 0, 1, 0])
        print(res)

    def test_update(self):
        self.query.update(512, *[10000, None, None, None, None, None])
        self.query.update(512, *[None, 4096, 2048, None, None, None])
        self.query.update(512, *[None, None, 2048, None, None, None])
        self.query.update(512, *[None, None, None, 1024, None, None])
        self.query.update(512, *[None, None, None, None,  512, None])
        self.query.update(512, *[None, None, None, None, None,  256])


        for col_id in range(len(self.table.page_directory["Tail"])):
             for p_val in self.table.page_directory["Tail"][col_id]:
                 print("{}".format(col_id))
                 print(p_val.data[0:100])
        #for col_id in range(len(self.table.page_directory["Base"])):
        #    for p_val in self.table.page_directory["Base"][col_id]:
        #        print("{}".format(col_id))
        #        print(p_val.data[0:100])


    def run_all(self):
        self.test_insert()
        self.test_update()
        self.test_select()

def main():
    query_tester = Query_Tester()
    query_tester.run_all()

if __name__ == "__main__":
    main()

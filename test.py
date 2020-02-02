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

        for i in range(513):
            self.query.insert(i, 1+i, 2+i, 3+i, 4+i, 5+i)
        
        # for col_id in range(len(self.table.page_directory["Base"])):
        #     for p_val in self.table.page_directory["Base"][col_id]:
        #         print("{}".format(col_id))
        #         print(p_val.data[0:100])


    def test_select(self):
        for i in range(513):
            res = self.query.select(i, [1, 1, 1, 1, 1, 1, 1])
            print(i, res)

        # for col_id in range(len(self.table.page_directory["Base"])):
        #     for p_val in self.table.page_directory["Base"][col_id]:
        #         print("{}".format(col_id))
        #         print(p_val.data[0:100])


    def run_all(self):
        self.test_insert()
        self.test_select()


def main():
    query_tester = Query_Tester()
    query_tester.run_all()

if __name__ == "__main__":
    main()
import os
from template.page import Page
from template.table import Table
from template.query import Query
from template.db import Database


class Query_Tester:
    def __init__(self):
        self.db = Database()
        self.table = self.db.create_table("test", 6, 0)    

    def test_query(self):
        query = Query(self.table)

        for i in range(513):
            query.insert(i, 0, 1, 2, 3, 4)
        
        for col_id in range(len(self.table.page_directory["Base"])):
            for p_val in self.table.page_directory["Base"][col_id]:
                print("{}".format(col_id))
                print(p_val.data[0:100])


    def run_all(self):
        self.test_query()

def main():
    query_tester = Query_Tester()
    query_tester.run_all()

if __name__ == "__main__":
    main()
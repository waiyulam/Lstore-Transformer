from template.page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
# TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 2


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.num_updates = 0
        self.__init_pages()

    def __init_pages(self):
        self.page_directory = {
            "Base": {},
            "Tail": {}
        }

        for i in range(self.num_columns + 3):
            self.page_directory["Base"][i] = [Page()]
            self.page_directory["Tail"][i] = [Page()]
    
    def get(self,rid):
        rid_page = self.page_directory["Base"][RID_COLUMN]
        for i in range(len(rid_page)):
            for j in range(rid_page[i].num_records):
                if (rid_page[i].get(j) == rid):
                    record_index = j
                    record_page_index = i
                    break
        return record_page_index,record_index



    def __merge(self):
        pass
 

from template.page import *
from template.config import *
from time import time

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.indirection = None

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
        self.num_records = 0
        self.__init_pages()

    def __init_pages(self):
        self.page_directory = {
            "Base": {},
            "Tail": {}
        }

        for i in range(self.num_columns + 3):
            self.page_directory["Base"][i] = [Page()]
            self.page_directory["Tail"][i] = [Page()]

    def get(self,key):
        key_page = self.page_directory["Base"][3 + self.key]
        for i in range(len(key_page)):
            for j in range(key_page[i].num_records):
                if (key_page[i].get(j) == (key).to_bytes(8, byteorder='big')):
                    record_index = j
                    record_page_index = i
                    break
        return record_page_index,record_index
    
    def key_to_rid(self, key):
        page_index, record_index = self.get(self, key)
        rid_page = self.page_directory["Base"][RID_COLUMN]
        return rid_page[page_index][record_index] # in bytes

    def index_to_key(self, index):
        key_page = self.page_directory["Base"][3 + self.key]
        page_index = index // MAX_RECORDS
        record_index = index % MAX_RECORDS
        return key_page[page_index][record_index] # in bytes

    def __merge(self):
        pass

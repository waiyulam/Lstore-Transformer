from template.page import *
from template.config import *
from template.index import Index
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
        self.index = Index(self) # newly added
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

    # Get record physical locations
    # !!! assume key exists
    def get(self,key):
        key_page = self.page_directory["Base"][3 + self.key]
        record_index = 0
        record_page_index = 0
        for i in range(len(key_page)):
            for j in range(key_page[i].num_records):
                if (key_page[i].get(j) == (key).to_bytes(8, byteorder='big')):
                    record_index = j
                    record_page_index = i
                    break
        return record_page_index,record_index

    # want to find physical location of tail record given tid 
    # tid : bytesarray
    def get_tail(self,tid):
        tid_page = self.page_directory["Tail"][RID_COLUMN]
        record_index = 0
        record_page_index = 0
        for i in range(len(tid_page)):
            for j in range(tid_page[i].num_records):
                if (tid_page[i].get(j) == tid):
                    record_index = j
                    record_page_index = i
                    break
        return record_page_index,record_index

    # return the columns of attributes given tail record 
    def get_tail_columns(self,tid):
        tid_page = self.page_directory["Tail"][RID_COLUMN]
        record_index = 0
        record_page_index = 0
        columns = []
        for i in range(len(tid_page)):
            for j in range(tid_page[i].num_records):
                if (tid_page[i].get(j) == tid):
                    for k in range(0,self.num_columns):
                        columns.append(int.from_bytes(self.page_directory["Tail"][k+NUM_METAS][i].get(j),byteorder='big'))
                    break
        return columns

    
    def key_to_rid(self, key):
        page_index, record_index = self.get(key)
        rid_page = self.page_directory["Base"][RID_COLUMN]
        return rid_page[page_index].get(record_index) # in bytes
    
    def key_to_indirect(self,key):
        page_index, record_index = self.get(key)
        indirect_page = self.page_directory["Base"][INDIRECTION_COLUMN]
        return indirect_page[page_index].get(record_index) # in bytes


    def index_to_key(self, index):
        key_page = self.page_directory["Base"][3 + self.key]
        page_index = index // MAX_RECORDS
        record_index = index % MAX_RECORDS
        return key_page[page_index].get(record_index) # in bytes

    def __merge(self):
        pass

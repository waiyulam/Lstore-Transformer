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
        # TODO: invalid input -> columns > MAX_COLUMNS
        self.page_directory = {}
        #self.index = Index(self) # newly added
        self.num_updates = 0
        self.num_records = 0
        self.num_ranges = 0
        self.__init_pages()

    def __init_pages(self):
        self.page_directory = {
            "Base": {},
            "Tail": {}
        }

        for i in range(self.num_columns + NUM_METAS):
            self.page_directory["Base"][i] = [Page()]
            self.page_directory["Tail"][i] = [Page()]

    # Get record physical locations
    # !!! assume key exists
    def get(self,key):
        key_page = self.page_directory["Base"][NUM_METAS + self.key]
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
    def get_tail(self,tid,column):
        record_index = 0
        record_page_index = 0
        tid_str = str(tid.decode()).split('t')[1]
        tid = int(tid_str)
        return int.from_bytes(self.page_directory["Tail"][column+NUM_METAS][tid//MAX_RECORDS].get(tid%MAX_RECORDS),byteorder='big')

    # return the columns of attributes given tail record
    def get_tail_columns(self, tid):
        record_index = 0
        record_page_index = 0
        columns = []
        tid_str = str(tid.decode()).split('t')[1]
        tid = int(tid_str)
        for k in range(0,self.num_columns):
            columns.append(int.from_bytes(self.page_directory["Tail"][k+NUM_METAS][tid//MAX_RECORDS].get(tid%MAX_RECORDS),byteorder='big'))
        return columns


    """ invalidating the record : set bid and tids of this record to 0"""
    def invalidate_record(self, page_index, record_index):
        # invalidate the bid
        rid_page = self.page_directory["Base"][RID_COLUMN]
        rid_page[page_index].data[record_index*8:(record_index+1)*8] = (0).to_bytes(8, byteorder='big')
        # invalidate the tid
        tid_page = self.page_directory["Tail"][RID_COLUMN]
        byte_indirect = self.page_directory["Base"][INDIRECTION_COLUMN][page_index].get(record_index)
        while ('b' not in byte_indirect.decode()) & (byte_indirect != MAXINT.to_bytes(8,byteorder = "big")):
            tid_str = str(byte_indirect.decode()).split('t')[1]
            tid = int(tid_str)
            page_inde,record_index = tid//MAX_RECORDS,tid%MAX_RECORDS
            tid_page[page_index].data[record_index*8:(record_index+1)*8] = (0).to_bytes(8, byteorder='big')
            byte_indirect = self.page_directory["Tail"][INDIRECTION_COLUMN][page_index].get(record_index)

    def page_write(self, data, type):
        for i, value in enumerate(data):
            page = self.page_directory[type][i][-1]
            # Verify Page is not full
            if not page.has_capacity():
                self.page_directory[type][i].append(Page())
                page = self.page_directory[type][i][-1]
            page.write(value)


    def __merge(self):
        pass

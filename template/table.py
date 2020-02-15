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

    # return the specific column of the table with respective rid column, optional argument for debugging
    # 0 for INDIRECTION_COLUMN , 1 for RID_COLUMN, 2 for SCHEMA_ENCODING_COLUMN, 3 for nothing
    def get_column(self, key, check_key):
        #print(self.page_directory["Base"][6][0].data)
        encoding_page = self.page_directory["Base"][SCHEMA_ENCODING_COLUMN]
        indirection_page = self.page_directory["Base"][INDIRECTION_COLUMN]
        column = []
        additional_column = []
        for i in range(len(indirection_page)):
            for j in range(indirection_page[i].num_records):
                schema_encoding = int.from_bytes(encoding_page[i].get(j),byteorder="big")
                if (schema_encoding & (1<<key))>>key == 1:
                    indir_String = indirection_page[i].get(j).decode()
                    str_num = str(indir_String).split('t')[1]
                    indir_int = int(str_num)
                    value = self.page_directory["Tail"][key+3][indir_int//MAX_RECORDS].get(indir_int%MAX_RECORDS)
                    column.append(value)
                    if check_key < 3:
                        additional_column.append(self.page_directory["Tail"][check_key][indir_int//MAX_RECORDS].get(indir_int%MAX_RECORDS))
                else:
                    column.append(self.page_directory["Base"][key+3][i].get(j))
                    if check_key < 3:
                        additional_column.append(self.page_directory["Base"][check_key][i].get(j))
        return column, additional_column

    # return the specific column of the table
    def get_old_column(self, key):
        indirection_page = self.page_directory["Base"][INDIRECTION_COLUMN]
        schema_encoding_page = self.page_directory["Base"][SCHEMA_ENCODING_COLUMN]
        column = []
        for i in range(len(indirection_page)):
            for j in range(indirection_page[i].num_records):
                indir_num = int.from_bytes(indirection_page[i].get(j), byteorder="big")
                schema_encoding = int.from_bytes(schema_encoding_page[i].get(j),byteorder="big")
                if indir_num != MAXINT and (schema_encoding & (1<<key))>>key == 1:
                    indir_String = indirection_page[i].get(j).decode()
                    str_num = str(indir_String).split('t')[1]
                    indir_int = int(str_num)
                    column.append(self.page_directory["Tail"][key+3][indir_int//MAX_RECORDS].get(indir_int%MAX_RECORDS))
                else:
                    column.append(self.page_directory["Base"][key+3][i].get(j))
        return column

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


    def __merge(self):
        pass

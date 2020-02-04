from template.table import Table, Record
from template.index import Index
from template.page import Page
from template.config import *
from copy import copy
import re

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        pass

    """
    # Insert a record with specified columns
    # param *columns: list of integers      # contain list of (key,value) of record 
    """

    def insert(self, *columns):
        columns = list(columns)
        rid = int.from_bytes(('b'+ str(self.table.num_records)).encode(), byteorder = "big")
        #rid = columns[self.table.key]
        schema_encoding = int('0' * self.table.num_columns)
        # INDIRECTION+RID+SCHEMA_ENCODING
        meta_data = [MAXINT,rid,schema_encoding]
        columns = list(columns)
        meta_data.extend(columns)
        base_data = meta_data
        for i, value in enumerate(base_data):
            page = self.table.page_directory["Base"][i][-1]
            # Verify Page is not full
            if not page.has_capacity():
                self.table.page_directory["Base"][i].append(Page())
                page = self.table.page_directory["Base"][i][-1]
            page.write(value)
        self.table.num_records += 1

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        is_base = True
        indirect_id = MAXINT
        # Get the indirection id given choice of primary keys
        indirect_byte = self.table.key_to_indirect(key)
        # get physical location in base page for this key 
        page_rid,rec_rid = self.table.get(key)
        # Total record specified by key and columns : TA tester consider duplicated key? 
        records = []
        if(int.from_bytes(indirect_byte,byteorder = 'big') != MAXINT):
            is_base = False 
            # get physical location for tail record 
            page_tid,rec_tid = self.table.get_tail(indirect_byte)
        res = []
        for query_col, val in enumerate(query_columns):
            # column is not selected 
            if val != 1:
                res.append(None)
                continue
            # The column of this record is not updated 
            if is_base:
                res.append(int.from_bytes(self.table.page_directory["Base"][query_col + NUM_METAS][page_rid].get(rec_rid), byteorder="big"))    
            # The column of this record has been updated : need to track tail record 
            else:
                b = self.table.page_directory["Tail"][query_col+NUM_METAS][page_tid].get(rec_tid)
                value = int.from_bytes(b,byteorder = 'big')
                # such column(attributes) in this record is not updated 
                if (value == MAXINT):
                    res.append(int.from_bytes(self.table.page_directory["Base"][query_col + NUM_METAS][page_rid].get(rec_rid), byteorder="big"))
                else:
                    res.append(int.from_bytes(self.table.page_directory["Tail"][query_col + NUM_METAS][page_tid].get(rec_tid), byteorder="big"))
        
        record = Record(self.table.key_to_rid(key).decode(),key,res)
        records.append(record)
        return records

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        # get the indirection in base pages given specified key 
        base_indirection_id = self.table.key_to_indirect(key) 
        update_record_page_index,update_record_index = self.table.get(key)
        for query_col,val in enumerate(columns):
            if val == None:
                continue 
            else:
                # compute new tail record TID 
                next_tid = int.from_bytes(('t'+ str(self.table.num_updates)).encode(), byteorder = "big")
                # the record is firstly updated 
                if (int.from_bytes(base_indirection_id,byteorder='big') == MAXINT):
                    # compute new tail record indirection :  the indirection of tail record point backward to base pages
                    next_tail_indirection = self.table.key_to_rid(key)
                    next_tail_indirection = int.from_bytes(next_tail_indirection,byteorder='big')
                    # compute tail columns : e.g. [NONE,NONE,updated_value,NONE]
                    next_tail_columns = []
                    next_tail_columns = [MAXINT for i in range(0,len(columns))]
                    next_tail_columns[query_col] = val
                    
                # the record has been updated 
                else:
                    # compute new tail record indirection : the indirection of new tail record point backward to last tail record for this key
                    next_tail_indirection = int.from_bytes(base_indirection_id,byteorder='big')
                    # compute tail columns : first copy the columns of the last tail record and update the new specified attribute 
                    next_tail_columns = self.table.get_tail_columns(base_indirection_id)
                    next_tail_columns[query_col] = val
                # !!!: Need to do the encoding for lastest update
                schema_encoding = int('0' * self.table.num_columns)
                # update new tail record 
                meta_data = [next_tail_indirection,next_tid,schema_encoding]
                meta_data.extend(next_tail_columns)
                tail_data = meta_data
                for col_id, col_val in enumerate(tail_data):
                    page = self.table.page_directory["Tail"][col_id][-1]
                    # Verify tail Page is not full
                    if not page.has_capacity():
                        self.table.page_directory["Tail"][col_id].append(Page())
                        page = self.table.page_directory["Tail"][col_id][-1]
                    page.write(col_val)
                # overwrite base page with new metadata 
                self.table.page_directory["Base"][INDIRECTION_COLUMN][update_record_page_index].update(update_record_index, next_tid)
                self.table.num_updates += 1

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int   # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        selected_keys = [] # selected keys in bytes
        start_index = self.table.key_to_index(start_range)
        end_index = self.table.key_to_index(end_range)
        for index in range(start_index, end_index + 1):
            selected_keys.append(int.from_bytes(self.table.index_to_key(index), byteorder = "big"))
        result = 0
        for key in selected_keys:
            encoder = [0] * self.table.num_columns
            encoder[aggregate_column_index] = 1
            result += (self.select(key, encoder))[0].columns[aggregate_column_index]
        return result

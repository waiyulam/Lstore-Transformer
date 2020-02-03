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
    """

    def insert(self, *columns):
        columns = list(columns)
        encode_rid = ('b'+ str(self.table.num_records)).encode("utf-8")
        rid = int.from_bytes(encode_rid, byteorder = "big")
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
            while not page.has_capacity():
                self.table.page_directory["Base"][i].append(Page())
                page = self.table.page_directory["Base"][i][-1]
            page.write(value)
        self.table.num_records += 1

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        key_col_id = self.table.key # int
        pages = self.table.page_directory["Base"][key_col_id + 3]
        b_key = (key).to_bytes(8, byteorder='big')

        is_base = True
        page_id = 0
        rec_id = 0
        for i in range(len(pages)):
            for j in range(pages[i].num_records):
                if (pages[i].get(j) == b_key):
                    page_id = i
                    rec_id = j
                    break

        rec_id_string = self.table.page_directory["Base"][INDIRECTION_COLUMN][page_id].get(rec_id).decode()
        rec_id_string = rec_id_string[rec_id_string.rfind('t'):]
        int_rec_id = int(rec_id_string[1:])
        if  int_rec_id != MAXINT:
            is_base = False
            page_id = int(int_rec_id / 512)
            rec_id = int_rec_id % 512

        res = []
        for query_col, val in enumerate(query_columns):
            if val != 1:
                res.append(None)
                continue

            if is_base:
                res.append(int.from_bytes(self.table.page_directory["Base"][query_col + 3][page_id].get(rec_id), byteorder="big"))
            else:
                print(page_id, rec_id, query_col)
                ret_val_string = self.table.page_directory["Tail"][query_col + 3][page_id].get(rec_id).decode()
                ret_val_string = ret_val_string[ret_val_string.rfind('t'):]
                ret_val = int(ret_val_string[1:])
                page_id2 = copy(page_id)
                rec_id2 = copy(rec_id)
                while ret_val == MAXINT:
                    int_rec_id = page_id2 * 512 + rec_id2 - 1
                    page_id2 = int(int_rec_id / 512)
                    rec_id2 = int_rec_id % 512
                    ret_val_string = self.table.page_directory["Tail"][query_col + 3][page_id].get(rec_id2).decode()
                    ret_val_string = ret_val_string[ret_val_string.rfind('t'):]
                    ret_val = int(ret_val_string[1:])

                res.append(ret_val)

        return res

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        key_col_id = self.table.key # int
        key_pages = self.table.page_directory["Base"][3+key_col_id]
        indirection_pages = self.table.page_directory["Base"][INDIRECTION_COLUMN]
        update_record_index = 0
        update_record_page_index = 0
        b_key = (key).to_bytes(8, byteorder='big')

        indirection_id = MAXINT
        for i in range(len(key_pages)):
            for j in range(key_pages[i].num_records):
                if (key_pages[i].get(j) == b_key):
                    update_record_index = j
                    update_record_page_index = i
                    break

        indirection_id = self.table.page_directory["Base"][INDIRECTION_COLUMN][update_record_page_index].get(update_record_index)

        int_indirection_id = int.from_bytes(indirection_id, byteorder="big")

        #tail_indirection_id = int_indirection_id
        #if tail_indirection_id == MAXINT:
    #        tail_indirection_id = int.from_bytes(self.table.page_directory["Base"][RID_COLUMN][update_record_page_index].get(update_record_index), byteorder = "big")

        #tid = self.table.num_updates
        tid = int.from_bytes(('t'+ str(self.table.num_updates)).encode(), byteorder = "big")
        if int_indirection_id == MAXINT:
            int_indirection_id = self.table.page_directory["Base"][RID_COLUMN][update_record_page_index].get(update_record_index)
            int_indirection_id = int.from_bytes(int_indirection_id, byteorder="big")
        # !!!: Need to do the encoding for lastest update
        schema_encoding = int('0' * self.table.num_columns)
        # INDIRECTION+tid
        meta_data = [int_indirection_id,tid,schema_encoding]
        list_columns = list(columns)
        meta_data.extend(list_columns)
        tail_data = meta_data
        for col_id, col_val in enumerate(tail_data):
            if col_val == None:
                col_val = MAXINT
            # Create New Page if current tail of tail page if fulled
            if not self.table.page_directory["Tail"][col_id][-1].has_capacity():
                self.table.page_directory["Tail"][col_id].append(Page())

            self.table.page_directory["Tail"][col_id][-1].write(col_val)

        #if int_indirection_id == MAXINT:
            #int_indirection_id = tid
    #    else:
        #    int_indirection_id += 1
        self.table.page_directory["Base"][INDIRECTION_COLUMN][update_record_page_index].update(update_record_index, tid)
        self.table.num_updates += 1




    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int   # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        # end_range is the number of keys should be selected
        selected_keys = [] # selected keys in bytes
        end_index = start_range + end_range
        for index in range(start_range, end_index):
            selected_keys.append(int.from_bytes(self.table.index_to_key(index), byteorder = "big"))
        return selected_keys
        
        # uncomment after fixing select
        # result = 0
        # for key in selected_keys:
        #     encoder = [0] * self.table.num_columns
        #     encoder[aggregate_column_index] = 1
        #     result += (self.select(key, encoder))[aggregate_column_index]
        # return result

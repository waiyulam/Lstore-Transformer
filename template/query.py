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
        page_index, record_index = self.table.get(key)
        self.table.invalidate_rid(page_index, record_index)
        #TODO: go through the indirection and invalidate all the tale records rid
        #TODO: need testing
        indirect_page = self.table.page_directory["Base"][INDIRECTION_COLUMN]
        indirect_tail_page = self.table.page_directory["Tail"][INDIRECTION_COLUMN]
        byte_indirect = indirect_page[page_index].get(record_index)
        if byte_indirect != MAXINT.to_bytes(8,byteorder = "big"):
            string_indirect = byte_indirect.decode()
            tail_page_index, tail_record_index = self.table.get_tail(byte_indirect)
            self.table.invalidate_tid(tail_page_index, tail_record_index)
            tail_byte_indirect = indirect_tail_page[tail_page_index].get(tail_record_index)
            tail_string_indirect = tail_byte_indirect.decode()
            while 'b' not in tail_string_indirect:
                tail_page_index, tail_record_index = self.table.get_tail(tail_byte_indirect)
                self.table.invalidate_tid(tail_page_index, tail_record_index)
                tail_byte_indirect = indirect_tail_page[tail_page_index].get(tail_record_index)
                if tail_byte_indirect != MAXINT.to_bytes(8,byteorder = "big"):
                    tail_string_indirect = tail_byte_indirect.decode()
                #print(tail_string_indirect)
            tail_page_index, tail_record_index = self.table.get_tail(tail_byte_indirect)
            self.table.invalidate_tid(tail_page_index, tail_record_index)
                #print(string_indirect)
                #if 'b' in tail_string_indirect:
                #    break;

    """
    # Insert a record with specified columns
    # param *columns: list of integers      # contain list of (key,value) of record
    """

    def insert(self, *columns):
        columns = list(columns)
        rid = int.from_bytes(('b'+ str(self.table.num_records)).encode(), byteorder = "big")
        #rid = columns[self.table.key]
        schema_encoding = '0' * self.table.num_columns
        schema_encoding = int.from_bytes(schema_encoding.encode(),byteorder = 'big')
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
        self.table.keys.append(columns[self.table.key])

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        # Get the indirection id given choice of primary keys
        indirect_byte = self.table.key_to_indirect(key)
        # Total record specified by key and columns : TA tester consider duplicated key?
        records, res = [], []
        schema_encoding = self.table.get_schema_encoding(key).decode()
        invalid_bits = 8 - self.table.num_columns
        for query_col, val in enumerate(query_columns):
            # column is not selected
            if val != 1:
                res.append(None)
                continue
            # print(schema_encoding)
            if schema_encoding[invalid_bits+query_col] == '1':
                # print("Column {} Modified. Read from Tail".format(query_col))
                page_tid, rec_tid = self.table.get_tail(indirect_byte)
                res.append(int.from_bytes(self.table.page_directory["Tail"][query_col + NUM_METAS][page_tid].get(rec_tid), byteorder="big"))
            else:
                # print("Column {} Not Modified. Read from Head".format(query_col))
                page_rid, rec_rid = self.table.get(key)
                res.append(int.from_bytes(self.table.page_directory["Base"][query_col + NUM_METAS][page_rid].get(rec_rid), byteorder="big"))

        # Uncommented to view result
        # print(res)
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
                temp_encoding = ["0"] * len(columns)
                temp_encoding[query_col] = "1"
                temp_encoding = int.from_bytes(("".join(temp_encoding)).encode(),byteorder = 'big')
                old_encoding = int.from_bytes(self.table.get_schema_encoding(key),byteorder = 'big')
                schema_encoding = temp_encoding|old_encoding
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
                self.table.page_directory["Base"][SCHEMA_ENCODING_COLUMN][update_record_page_index].update(update_record_index, schema_encoding)
                self.table.num_updates += 1
    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int   # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        keys = sorted(self.table.keys)
        start_index = 0
        end_index = 0
        count = 0
        for key in keys:
            if key == start_range:
                start_index = count
            elif key == end_range:
                end_index = count
            count += 1
        selected_keys = keys[start_index : end_index + 1]
        result = 0
        for key in selected_keys:
            encoder = [0] * self.table.num_columns
            encoder[aggregate_column_index] = 1
            result += (self.select(key, encoder))[0].columns[aggregate_column_index]
        return result

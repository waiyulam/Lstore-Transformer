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
        rid = columns[self.table.key]
        schema_encoding = int('0' * self.table.num_columns)
        # INDIRECTION+RID+SCHEMA_ENCODING
        meta_data = [MAXINT,rid,schema_encoding]
        columns = list(columns)
        meta_data.extend(columns)
        base_data = meta_data
        for i, value in enumerate(base_data):
            page = self.table.page_directory["Base"][i][-1]
<<<<<<< HEAD
=======

>>>>>>> cfad13bacb9d2f14a337516cbb1b58491cc1ea43
            # Verify Page is not full
            while not page.has_capacity():
                self.table.page_directory["Base"][i].append(Page())
                page = self.table.page_directory["Base"][i][-1]
<<<<<<< HEAD
=======

>>>>>>> cfad13bacb9d2f14a337516cbb1b58491cc1ea43
            page.write(value)

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        key_col_id = self.table.key # int
        pages = self.table.page_directory["Base"][key_col_id]
<<<<<<< HEAD
=======

>>>>>>> cfad13bacb9d2f14a337516cbb1b58491cc1ea43
        b_key = (key).to_bytes(8, byteorder='big')

        page_id = 0
        rec_id = 0
        for i in range(len(pages)):
            for j in range(pages[i].num_records):
                if (pages[i].get(j) == b_key):
                    page_id = i
                    rec_id = j
                    break

        res = []
        for query_col, val in enumerate(query_columns):
            if val != 1:
                continue
            res.append(int.from_bytes(self.table.page_directory["Base"][query_col][page_id].get(rec_id), byteorder="big"))

        return res

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        key_col_id = self.table.key # int
        key_pages = self.table.page_directory["Base"][3+key_col_id]
        indirection_pages = self.table.page_directory["Base"][0]
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

        indirection_id = self.table.page_directory["Base"][0][update_record_page_index].get(update_record_index)

        int_indirection_id = int.from_bytes(indirection_id, byteorder="big")


        tid = self.table.num_updates
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

        if int_indirection_id == MAXINT:
            int_indirection_id = 0
        else:
            int_indirection_id += 1
        self.table.page_directory["Base"][0][update_record_page_index].update(update_record_index, int_indirection_id)
        self.table.num_updates += 1



    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int   # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass

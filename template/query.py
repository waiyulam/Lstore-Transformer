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
        schema_encoding = int('0' * self.table.num_columns)
        columns = list(columns)
        columns.extend([schema_encoding, MAXINT])
        for i, value in enumerate(columns):
            page = self.table.page_directory["Base"][i][-1]
            
            # Verify Page is not full
            while not page.has_capacity():
                self.table.page_directory["Base"][i].append(Page())
                page = self.table.page_directory["Base"][i][-1]
            
            page.write(value)
        
    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        key_col_id = self.table.key # int
        pages = self.table.page_directory["Base"][key_col_id]
        
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
        pages = self.table.page_directory["Base"][key_col_id]
        
        b_key = (key).to_bytes(8, byteorder='big')
        
        indirection_id = MAXINT
        for i in range(len(pages)):
            for j in range(pages[i].num_records):
                if (pages[i].get(j) == b_key):
                    indirection_id = self.table.page_directory["Base"][-1].get(j)

        self.table.num_updates += 1

        columns.extend([self.table.num_updates, indirection_id])
        for col_id, col_val in enumerate(columns):
            # Create New Page if current tail of tail page if fulled
            if not self.page_directory["Tail"][col_id][-1].has_capacity():
                self.page_directory["Tail"][col_id].append(Page())
            
            self.page_directory["Tail"][col_id][-1].write(col_val)

        if indirection_id == MAXINT:
            indirection_id = 0
        else
            indirection_id += 1

        self.table.page_directory["Base"][-1].get(j) = indirection_id

        



        

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int   # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass

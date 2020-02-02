from template.table import Table, Record
from template.index import Index
from template.page import Page
from template.config import * 
from copy import copy

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
        pass

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int   # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass

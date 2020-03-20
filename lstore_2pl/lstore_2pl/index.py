from lstore_2pl.config import *
from lstore_2pl.buffer_pool import BufferPool
from BTrees.OOBTree import OOBTree

from functools import reduce
from operator import add
import re
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
# The indexing is implemented as B Tree and for each node, we have (key, page pointer) mapping

class Index:

    def __init__(self, table):
        self.table = table
        # One index for each column. All our empty initially.
        self.indices = [None] *  table.num_columns
        # Create default index for key columns
        tree = OOBTree()
        self.indices[self.table.key] = tree

    """ insert new entry or collection of entires to index of specified column """
    def update_index(self, key, pointer, column_number):
        if not self.indices[column_number].has_key(key):
            # add new key to tree
            pointers = []
            pointers.append(pointer)
            self.indices[column_number].insert(key,pointers)
        else:
            self.indices[column_number][key].append(pointer)


    """
    # returns the location of all records with the given value on column "column", list of RID locations
    """

    def locate(self, column, value):
        tree = self.indices[column]
        if tree.has_key(value):
            return tree[value]
        return None

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if begin > end:
            return list(self.indices[column].values(min=end, max=begin))[::-1]
        else:
            return list(self.indices[column].values(min=begin, max=end))

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        tree = OOBTree()
        self.indices[column_number] = tree
        # Look through the specific column-based columns 
        for i in range(self.table.num_records):
            # Compute Base page pointers 
            range_indice = i// (MAX_RECORDS * PAGE_RANGE)
            range_remainder = i % (MAX_RECORDS * PAGE_RANGE)
            page_pointer = [range_indice, range_remainder//MAX_RECORDS, range_remainder%MAX_RECORDS]
            # Find Schema encoding to find the lastest column value of this record 
            args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, *page_pointer ]
            base_schema = int.from_bytes(BufferPool.get_record(*args), byteorder='big')
            # Find Indirection 
            args = [self.table.name, "Base", INDIRECTION_COLUMN, *page_pointer]
            base_indirection = BufferPool.get_record(*args)
            # Find column value 
            if (base_schema & (1<<column_number))>>column_number == 1:
                key = self.table.get_tail(int.from_bytes(base_indirection,byteorder = 'big'),column_number, page_pointer[0])
            else:
                args = [self.table.name, "Base", column_number + NUM_METAS, *page_pointer]
                key = (int.from_bytes(BufferPool.get_record(*args), byteorder="big"))
            self.update_index(key, page_pointer, column_number)
            



    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None

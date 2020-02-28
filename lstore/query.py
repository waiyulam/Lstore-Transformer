from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from lstore.buffer_pool import BufferPool
from lstore.config import *
from lstore.page_range import *
from copy import copy
import re
from time import time
import datetime
from functools import reduce
from operator import add
import threading
# TODO: Change RID to all integer and set offset bit
# TODO : implement all queries by indexing
# TODO : implement page range
# TODO : support non primary key selection


def datetime_to_int(dt):
    return int(dt.strftime("%Y%m%d%H%M%S"))

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        self.table = table
        #self.table.index = Index(self.table)
        # pointer contains page range, page number, indices within page
        self.page_pointer = [0,0,0]
        pass

    """
    # Insert a record with specified columns
    # param *columns: list of integers      # contain list of (key,value) of record
    """

    def insert(self, *columns):
        columns = list(columns)
        rid = self.table.num_records
        #rid = int.from_bytes(('b'+ str(self.table.num_records)).encode(), byteorder = "big")
        schema_encoding = 0
        base_rid = rid
        #rid = int.from_bytes(('b'+ str(self.table.num_records)).encode(), byteorder = "big")
        starttime = datetime_to_int(datetime.datetime.now())
        lastupdatetime = 0
        updatetime = 0
        # INDIRECTION+RID+SCHEMA_ENCODING
        meta_data = [MAXINT,rid,schema_encoding,base_rid,starttime,lastupdatetime,updatetime]
        columns = list(columns)
        meta_data.extend(columns)
        base_data = meta_data
        self.table.base_page_write(base_data)
        # update indices
        range_indice = self.table.num_records // (MAX_RECORDS * PAGE_RANGE)
        range_remainder = self.table.num_records % (MAX_RECORDS * PAGE_RANGE)
        self.page_pointer = [range_indice, range_remainder//MAX_RECORDS, range_remainder%MAX_RECORDS]
        # update all existed index 
        for i in range(self.table.num_columns):
            if self.table.index.indices[i] != None:
                self.table.index.update_index(columns[i],self.page_pointer,i)

        # record_page_index,record_index = self.table.get(columns[self.table.key])
        # if (self.page_pointer != [record_page_index,record_index]):
        #     print("error message"+str(self.page_pointer) + str([record_page_index,record_index]))
        self.table.num_records += 1

    """
    # Read a record with specified key
    """
    ###
    # def select(self, key, column, query_columns):
    ###

    def select(self, key, column, query_columns):
        # Get the indirection id given choice of key in specific column
        page_pointer = self.table.index.locate(column, key)
        records = []
        for i in range(len(page_pointer)):
            # collect base meta datas of each record
            args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, *page_pointer[i]]
            base_schema = int.from_bytes(BufferPool.get_record(*args), byteorder='big')
            args = [self.table.name, "Base", INDIRECTION_COLUMN, *page_pointer[i]]
            base_indirection = BufferPool.get_record(*args)

            # Total record specified by key and columns
            res = []
            for query_col, val in enumerate(query_columns):
                # column is not selected
                if val != 1:
                    res.append(None)
                    continue
                if (base_schema & (1<<query_col))>>query_col == 1:
                    res.append(self.table.get_tail(int.from_bytes(base_indirection,byteorder = 'big'),query_col, page_pointer[i][0]))
                else:
                    args = [self.table.name, "Base", query_col + NUM_METAS, *page_pointer[i]]
                    res.append(int.from_bytes(BufferPool.get_record(*args), byteorder="big"))
            
            # construct the record with rid, primary key, columns
            args = [self.table.name, "Base", RID_COLUMN, *page_pointer[i]]
            rid = BufferPool.get_record(*args)
            args = [self.table.name, "Base", NUM_METAS + column, *page_pointer[i]]
            # or non_prim _key 
            prim_key = BufferPool.get_record(*args)
            record = Record(rid, prim_key, res)
            records.append(record)
        return records

    """
    # Update a record with specified key and columns
    """
    def update(self, key, *columns):
        # get the indirection in base pages given specified key\
        page_pointer = self.table.index.locate(self.table.key,key)
        update_range_index, update_record_page_index,update_record_index = page_pointer[0][0],page_pointer[0][1], page_pointer[0][2]
        # if primary key in index is also updated, then insert new entries into primary key index 
        if (columns[self.table.key] != None ):
             self.table.index.update_index(columns[self.table.key],page_pointer[0],self.table.key)
        args = [self.table.name, "Base", INDIRECTION_COLUMN, *page_pointer[0]]
        base_indirection_id = BufferPool.get_record(*args)
        args = [self.table.name, "Base", RID_COLUMN, *page_pointer[0]]
        base_rid = BufferPool.get_record(*args)
        base_id = int.from_bytes(base_rid, byteorder='big')

        for query_col,val in enumerate(columns):
            if val == None:
                continue
            else:
                # self.table.page_directory["Base"][NUM_METAS+query_col][update_range_index].Hash_insert(int.from_bytes(base_rid,byteorder='big'))
                # compute new tail record TID
                self.table.page_range_meta[query_col, update_range_index][1] += 1
                self.table.Hashmap[query_col, update_range_index] = {}
                self.table.Hashmap[query_col, update_range_index][base_id] = 1
                tmp_indice = self.table.get_latest_tail((INDIRECTION_COLUMN, update_range_index))
                args = [self.table.name, "Tail", INDIRECTION_COLUMN, update_range_index, tmp_indice]
                page_records = BufferPool.get_page(*args).num_records
                total_records = page_records + tmp_indice*MAX_RECORDS
                next_tid = total_records
                #next_tid = int.from_bytes(('t'+ str(total_records)).encode(), byteorder = "big")
                # the record is firstly updated
                if (int.from_bytes(base_indirection_id,byteorder='big') == MAXINT):
                    # compute new tail record indirection :  the indirection of tail record point backward to base pages
                    args = [self.table.name, "Base", RID_COLUMN, *page_pointer[0]]
                    next_tail_indirection = BufferPool.get_record(*args)  # in bytes
                    next_tail_indirection = int.from_bytes(next_tail_indirection, byteorder='big')
                    # compute tail columns : e.g. [NONE,NONE,updated_value,NONE]
                    next_tail_columns = []
                    next_tail_columns = [MAXINT for i in range(0,len(columns))]
                    next_tail_columns[query_col] = val
                # the record has been updated
                else:
                    # compute new tail record indirection : the indirection of new tail record point backward to last tail record for this key
                    next_tail_indirection = int.from_bytes(base_indirection_id,byteorder='big')
                    # compute tail columns : first copy the columns of the last tail record and update the new specified attribute
                    base_indirection = int.from_bytes(base_indirection_id, byteorder = 'big')
                    next_tail_columns = self.table.get_tail_columns(base_indirection, update_range_index)
                    next_tail_columns[query_col] = val

                args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, *page_pointer[0]]
                encoding_base = BufferPool.get_record(*args)
                old_encoding = int.from_bytes(encoding_base,byteorder="big")
                new_encoding = old_encoding | (1<<query_col)
                schema_encoding = new_encoding
                starttime = datetime_to_int(datetime.datetime.now())
                lastupdatetime = 0
                updatetime = 0
                # update new tail record
                meta_data = [next_tail_indirection,next_tid,schema_encoding,base_id,starttime,lastupdatetime,updatetime]
                meta_data.extend(next_tail_columns)
                tail_data = meta_data
                self.table.tail_page_write(tail_data, update_range_index)

                # overwrite base page with new metadata
                args = [self.table.name, "Base", INDIRECTION_COLUMN, page_pointer[0][0], page_pointer[0][1]]
                page = BufferPool.get_page(*args)
                page.update(update_record_index, next_tid)

                args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, page_pointer[0][0], page_pointer[0][1]]
                page = BufferPool.get_page(*args)
                page.update(update_record_index, schema_encoding)

                self.table.num_updates += 1
        #self.table.event.set()

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int   # Index of desired column to aggregate
        # this function is only called on the primary key.
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        values = 0
        # locate all keys in index
        locations = self.table.index.locate_range(start_range, end_range, self.table.key)
        # Aggregating columns specified
        for i in range(len(locations)):
            page_pointer = locations[i]
            # collect base meta datas of this record
            args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, *page_pointer[0]]
            base_schema = int.from_bytes(BufferPool.get_record(*args), byteorder='big')
            args = [self.table.name, "Base", INDIRECTION_COLUMN, *page_pointer[0]]
            base_indirection = BufferPool.get_record(*args)

            if (base_schema & (1<<aggregate_column_index))>>aggregate_column_index == 1:
                temp = self.table.get_tail(int.from_bytes(base_indirection, byteorder = 'big'),aggregate_column_index, locations[i][0][0])
                if (temp == MAXINT): # might be deleted
                    continue
                values  += temp
            else:
                args = [self.table.name, "Base", aggregate_column_index + NUM_METAS, *page_pointer[0]]
                temp = int.from_bytes(BufferPool.get_record(*args), byteorder="big")
                if (temp == MAXINT): # might be deleted
                    continue
                values += temp
        return values

    """
    # internal Method
    # Read a record with specified RID
    """

    # TODO : merging -> remove all invalidate record and key in index
    def delete(self, key):
        #page_pointer = self.table.index.locate(self.table.key,key)
        null_value = []
        for i in range(self.table.num_columns):
            null_value.append(MAXINT)

        #page_range, page_index, record_index = page_pointer[0],page_pointer[1], page_pointer[2]
        page_pointer = self.table.index.locate(self.table.key,key)
        update_range_index, update_record_page_index,update_record_index = page_pointer[0][0],page_pointer[0][1], page_pointer[0][2]

        args = [self.table.name, "Base", INDIRECTION_COLUMN, *page_pointer[0]]
        base_indirection_id = BufferPool.get_record(*args)
        args = [self.table.name, "Base", RID_COLUMN, *page_pointer[0]]
        base_rid = BufferPool.get_record(*args)
        base_id = int.from_bytes(base_rid, byteorder='big')

        tmp_indice = self.table.get_latest_tail((INDIRECTION_COLUMN, update_range_index))
        args = [self.table.name, "Tail", INDIRECTION_COLUMN, update_range_index, tmp_indice]
        page_records = BufferPool.get_page(*args).num_records
        total_records = page_records + tmp_indice*MAX_RECORDS
        next_tid = total_records
        #next_tid = int.from_bytes(('t'+ str(total_records)).encode(), byteorder = "big")

        # the record is firstly updated
        if (int.from_bytes(base_indirection_id,byteorder='big') == MAXINT):
            # compute new tail record indirection :  the indirection of tail record point backward to base pages
            args = [self.table.name, "Base", RID_COLUMN, *page_pointer]
            next_tail_indirection = BufferPool.get_record(*args)  # in bytes
            next_tail_indirection = int.from_bytes(next_tail_indirection, byteorder='big')
        else:
            next_tail_indirection = int.from_bytes(base_indirection_id,byteorder='big')

        args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, *page_pointer[0]]
        encoding_base = BufferPool.get_record(*args)  # in bytes
        old_encoding = int.from_bytes(encoding_base,byteorder="big")
        new_encoding = int('1'* self.table.num_columns, 2)
        schema_encoding = new_encoding
        starttime = datetime_to_int(datetime.datetime.now())
        lastupdatetime = 0
        updatetime = 0
        # update new tail record
        meta_data = [next_tail_indirection,next_tid,schema_encoding,base_id,starttime,lastupdatetime,updatetime]
        meta_data.extend(null_value)
        tail_data = meta_data
        self.table.tail_page_write(tail_data, update_range_index)

        # overwrite base page with new metadata
        args = [self.table.name, "Base", INDIRECTION_COLUMN, page_pointer[0][0], page_pointer[0][1]]
        page = BufferPool.get_page(*args)
        page.update(update_record_index, next_tid)

        args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, page_pointer[0][0], page_pointer[0][1]]
        page = BufferPool.get_page(*args)
        page.update(update_record_index, schema_encoding)
        self.table.num_updates += 1

    #    self.table.invalidate_record(page_range, page_index, record_index)

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
from queue import Queue
# TODO: Change RID to all integer and set offset bit
# TODO : implement all queries by indexing
# TODO : implement page range
# TODO : support non primary key selection


def datetime_to_int(dt):
    return int(dt.strftime("%Y%m%d%H%M%S"))

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        #self.table.index = Index(self.table)
        # pointer contains page range, page number, indices within page
        self.page_pointer = [0,0,0]
        pass

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
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

        # initialize the queues inside priority_queues
        for queue in self.table.priority_queues:
            queue[(self.page_pointer[0], self.page_pointer[1])] = Queue()

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
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    """
    # Based on indexes, breakdown select into read operations to every index records
    # return value will be a list of record dictionaries
    """
    def select(self, key, column, query_columns):
        self.table.select_count += 1
        # Get the indirection id given choice of key in specific column
        page_pointers = self.table.index.locate(column, key)
        #records = []
        ops_list = []

        for page_pointer in page_pointers:

            for index, val in enumerate(query_columns):
                if val == 1:
                    ops = {}
                    ops['command_type'] = "select"
                    ops['command_num'] = self.table.select_count
                    ops['column_id'] = index + NUM_METAS
                    ops['r_w'] = "read"
                    ops['base_tail'] = "base"
                    ops['meta_data'] = "data"
                    ops['page_pointer'] = page_pointer
                    ops['page_lacth'] = 0
                    #ops_list.append([index+NUM_METAS, ops])
                    ops_list.append([(page_pointer[0], page_pointer[1]), ops])
                    #
                    # ops = {}
                    # ops['command_type'] = "select"
                    # ops['command_num'] = self.table.select_count
                    # ops['column_id'] = index + NUM_METAS
                    # ops['r_w'] = "read"
                    # ops['base_tail'] = "tail"
                    # ops['meta_data'] = "data"
                    # ops['page_pointer'] = page_pointer
                    # ops['page_lacth'] = 0
                    # #ops_list.append([index+NUM_METAS, ops])
                    # ops_list.append([(page_pointer[0], page_pointer[1]), ops])
                    #print(ops_list)
        #read_indexes = []
        #for i in range(len(page_pointer)):
            # collect base meta datas of each record
        #    args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, *page_pointer[i]]
            #read_indexes.append(args)


        #    base_schema = int.from_bytes(BufferPool.get_record(*args), byteorder='big')
        #    args = [self.table.name, "Base", INDIRECTION_COLUMN, *page_pointer[i]]
        #    base_indirection = BufferPool.get_record(*args)

            # Total record specified by key and columns
        #    res = []
        #    for query_col, val in enumerate(column_id):
                # column is not selected
        #        if val != 1:
        #            res.append(None)
        #            continue
        #        if (base_schema & (1<<query_col))>>query_col == 1:
        #            res.append(self.table.get_tail(int.from_bytes(base_indirection,byteorder = 'big'),query_col, page_pointer[i][0]))
        #        else:
        #            args = [self.table.name, "Base", query_col + NUM_METAS, *page_pointer[i]]
        #            res.append(int.from_bytes(BufferPool.get_record(*args), byteorder="big"))

            # construct the record with rid, primary key, columns
        #    args = [self.table.name, "Base", RID_COLUMN, *page_pointer[i]]
        #    rid = BufferPool.get_record(*args)
        #    args = [self.table.name, "Base", NUM_METAS + column, *page_pointer[i]]
            # or non_prim _key
        #    prim_key = BufferPool.get_record(*args)
        #    record = Record(rid, prim_key, res)
        #    records.append(record)
        #print(ops_list)
        return ops_list
        #return records

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        self.table.update_count += 1
        ops_list = []
        # get the indirection in base pages given specified key\
        page_pointer = self.table.index.locate(self.table.key,key)
        #needs modify page_pointer
        update_range_index, update_record_page_index,update_record_index = page_pointer[0][0],page_pointer[0][1], page_pointer[0][2]
        # if primary key in index is also updated, then insert new entries into primary key index
        if (columns[self.table.key] != None ):
             self.table.index.update_index(columns[self.table.key],page_pointer[0],self.table.key)

        # read from meta-data columnn, indirection
        ops_temp = {}
        ops_temp['command_type'] = "update"
        ops_temp['command_num'] = self.table.update_count

        ops_temp['column_id'] = INDIRECTION_COLUMN
        ops_temp['r_w'] = "read"
        ops_temp['base_tail'] = "base"
        ops_temp['meta_data'] = "meta"
        ops_temp['page_pointer'] = page_pointer[0]
        ops_temp['page_lacth'] = 0
        ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])
        #ops_list.append([INDIRECTION_COLUMN, ops_temp])
        # read from meta-data column, rid
        ops_temp = {}
        ops_temp['command_type'] = "update"
        ops_temp['command_num'] = self.table.update_count

        ops_temp['column_id'] = RID_COLUMN
        ops_temp['r_w'] = "read"
        ops_temp['base_tail'] = "base"
        ops_temp['meta_data'] = "meta"
        ops_temp['page_pointer'] = page_pointer[0]
        ops_temp['page_lacth'] = 0
        ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])
        #ops_list.append([RID_COLUMN, ops_temp])
        #args = [self.table.name, "Base", INDIRECTION_COLUMN, *page_pointer[0]]
        #base_indirection_id = BufferPool.get_record(*args)
        # args = [self.table.name, "Base", RID_COLUMN, *page_pointer[0]]
        # base_rid = BufferPool.get_record(*args)
        # base_id = int.from_bytes(base_rid, byteorder='big')
        #update tailmetadata

        for query_col,val in enumerate(columns):
            #if val == None:
        #    else:
                # self.table.page_directory["Base"][NUM_METAS+query_col][update_range_index].Hash_insert(int.from_bytes(base_rid,byteorder='big'))
                # compute new tail record TID, it requires to latch the page, lacth the page means read this page

                # self.table.mg_rec_update(NUM_METAS+query_col, *page_pointer[0])
            tmp_indice = self.table.get_latest_tail(INDIRECTION_COLUMN, update_range_index)

            #args = [self.table.name, "Tail", INDIRECTION_COLUMN, update_range_index, tmp_indice]
            ops_temp = {}
            ops_temp['command_type'] = "update"
            ops_temp['command_num'] = self.table.update_count

            ops_temp['column_id'] = INDIRECTION_COLUMN
            ops_temp['r_w'] = "read"
            ops_temp['base_tail'] = "tail"
            ops_temp['meta_data'] = "meta"
            ops_temp['page_pointer'] = page_pointer[0]
            ops_temp['page_lacth'] = 1
            ops_temp['operation_column'] = query_col + NUM_METAS
            ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])
            #ops_list.append([INDIRECTION_COLUMN, ops_temp])
            #page_records = BufferPool.get_page(*args).num_records
            #total_records = page_records + tmp_indice*MAX_RECORDS
            #next_tid = total_records
            #next_tid = int.from_bytes(('t'+ str(total_records)).encode(), byteorder = "big")
            # the record is firstly updated

            # From first if branch, compute the read to get next tail indirection
            ops_temp = {}
            ops_temp['command_type'] = "update"
            ops_temp['command_num'] = self.table.update_count

            ops_temp['column_id'] = RID_COLUMN
            ops_temp['r_w'] = "read"
            ops_temp['base_tail'] = "base"
            ops_temp['meta_data'] = "meta"
            ops_temp['page_pointer'] = page_pointer[0]
            ops_temp['page_lacth'] = 0
            ops_temp['operation_column'] = query_col + NUM_METAS
            ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])
            #ops_list.append([RID_COLUMN, ops_temp])

            # write a tail record with specific column
            # needs write
            ops_temp = {}
            ops_temp['command_type'] = "update"
            ops_temp['command_num'] = self.table.update_count

            ops_temp['column_id'] = query_col + NUM_METAS
            ops_temp['r_w'] = "write"
            ops_temp['base_tail'] = "tail"
            ops_temp['meta_data'] = "data"
            ops_temp['page_pointer'] = page_pointer[0]
            ops_temp['page_lacth'] = 0
            if val == None:
                val = MAXINT
            ops_temp['write_data'] = val
            ops_temp['operation_column'] = query_col + NUM_METAS
            ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])
            #ops_list.append([query_col + NUM_METAS, ops_temp])
            # if (int.from_bytes(base_indirection_id,byteorder='big') == MAXINT):
            #     # compute new tail record indirection :  the indirection of tail record point backward to base pages
            #     args = [self.table.name, "Base", RID_COLUMN, *page_pointer[0]]
            #     next_tail_indirection = BufferPool.get_record(*args)  # in bytes
            #     next_tail_indirection = int.from_bytes(next_tail_indirection, byteorder='big')
            #     # compute tail columns : e.g. [NONE,NONE,updated_value,NONE]
            #     next_tail_columns = []
            #     next_tail_columns = [MAXINT for i in range(0,len(columns))]
            #     next_tail_columns[query_col] = val
            # # the record has been updated
            # else:
            #     # compute new tail record indirection : the indirection of new tail record point backward to last tail record for this key
            #     next_tail_indirection = int.from_bytes(base_indirection_id,byteorder='big')
            #     # compute tail columns : first copy the columns of the last tail record and update the new specified attribute
            #     base_indirection = int.from_bytes(base_indirection_id, byteorder = 'big')
            #     next_tail_columns = self.table.get_tail_columns(base_indirection, update_range_index)
            #     next_tail_columns[query_col] = val
            ops_temp = {}
            ops_temp['command_type'] = "update"
            ops_temp['command_num'] = self.table.update_count

            ops_temp['column_id'] = SCHEMA_ENCODING_COLUMN
            ops_temp['r_w'] = "read"
            ops_temp['base_tail'] = "base"
            ops_temp['meta_data'] = "meta"
            ops_temp['page_pointer'] = page_pointer[0]
            ops_temp['page_lacth'] = 0
            ops_temp['operation_column'] = query_col + NUM_METAS
            ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])
        #    ops_list.append([SCHEMA_ENCODING_COLUMN, ops_temp])

            #args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, *page_pointer[0]]
            #encoding_base = BufferPool.get_record(*args)
            #old_encoding = int.from_bytes(encoding_base,byteorder="big")
            #new_encoding = old_encoding | (1<<query_col)
            #schema_encoding = new_encoding
            #starttime = datetime_to_int(datetime.datetime.now())
            #lastupdatetime = 0
            #updatetime = 0
            # update new tail record
            # meta_data = [next_tail_indirection,next_tid,schema_encoding,base_id,starttime,lastupdatetime,updatetime]
            # meta_data.extend(next_tail_columns)
            # tail_data = meta_data
            # self.table.tail_page_write(tail_data, update_range_index)

            # page is already latched
            ops_temp = {}
            ops_temp['command_type'] = "update"
            ops_temp['command_num'] = self.table.update_count

            ops_temp['column_id'] = INDIRECTION_COLUMN
            ops_temp['r_w'] = "write"
            ops_temp['base_tail'] = "base"
            ops_temp['meta_data'] = "meta"
            ops_temp['page_pointer'] = page_pointer[0]
            ops_temp['page_lacth'] = 0
            ops_temp['operation_column'] = query_col + NUM_METAS
            ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])
            #ops_list.append([INDIRECTION_COLUMN, ops_temp])
            # overwrite base page with new metadata
            # args = [self.table.name, "Base", INDIRECTION_COLUMN, page_pointer[0][0], page_pointer[0][1]]
            # page = BufferPool.get_page(*args)
            # page.update(update_record_index, next_tid)
            ops_temp = {}
            ops_temp['command_type'] = "update"
            ops_temp['command_num'] = self.table.update_count

            ops_temp['column_id'] = SCHEMA_ENCODING_COLUMN
            ops_temp['r_w'] = "write"
            ops_temp['base_tail'] = "base"
            ops_temp['meta_data'] = "meta"
            ops_temp['page_pointer'] = page_pointer[0]
            ops_temp['page_lacth'] = 0
            ops_temp['operation_column'] = query_col + NUM_METAS
            ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])
                #ops_list.append([SCHEMA_ENCODING_COLUMN, ops_temp])
                # args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, page_pointer[0][0], page_pointer[0][1]]
                # page = BufferPool.get_page(*args)
                # page.update(update_record_index, schema_encoding)
        # self.table.update_count += 1

        self.table.num_updates += 1
        self.table.mergeThreadController()
        #self.table.event.set()
        return ops_list


    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        # values = 0
        self.table.sum_count += 1
        # locate all keys in index
        locations = self.table.index.locate_range(start_range, end_range, self.table.key)

        ops_list = []


        for i in range(len(locations)):
            page_pointer = locations[i]
            ops_temp = {}

            ops_temp['command_type'] = "sum"
            ops_temp['command_num'] = self.table.sum_count

            ops_temp['column_id'] = SCHEMA_ENCODING_COLUMN
            ops_temp['r_w'] = "read"
            ops_temp['base_tail'] = "base"
            ops_temp['meta_data'] = "meta"
            ops_temp['page_pointer'] = page_pointer[0]
            ops_temp['page_lacth'] = 0
            ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])
            #ops_list.append([SCHEMA_ENCODING_COLUMN, ops_temp])
            ops_temp = {}

            ops_temp['command_type'] = "sum"
            ops_temp['command_num'] = self.table.sum_count
            ops_temp['column_id'] = INDIRECTION_COLUMN
            ops_temp['r_w'] = "read"
            ops_temp['base_tail'] = "base"
            ops_temp['meta_data'] = "meta"
            ops_temp['page_pointer'] = page_pointer[0]
            ops_temp['page_lacth'] = 0
            ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])
        #    ops_list.append([INDIRECTION_COLUMN, ops_temp])
            ops_temp = {}

            ops_temp['command_type'] = "sum"
            ops_temp['command_num'] = self.table.sum_count
            ops_temp['column_id'] = aggregate_column_index+NUM_METAS
            ops_temp['r_w'] = "read"
            ops_temp['base_tail'] = "base"
            ops_temp['meta_data'] = "data"
            ops_temp['page_pointer'] = page_pointer[0]
            ops_temp['page_lacth'] = 0
            ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])

            #TODO: read tail data
            #ops_list.append([aggregate_column_index, ops_temp])

        return ops_list
        # Aggregating columns specified

        # for i in range(len(locations)):
        #     page_pointer = locations[i]
        #     # collect base meta datas of this record
        #     args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, *page_pointer[0]]
        #     base_schema = int.from_bytes(BufferPool.get_record(*args), byteorder='big')
        #     args = [self.table.name, "Base", INDIRECTION_COLUMN, *page_pointer[0]]
        #     base_indirection = BufferPool.get_record(*args)
        #
        #     if (base_schema & (1<<aggregate_column_index))>>aggregate_column_index == 1:
        #         temp = self.table.get_tail(int.from_bytes(base_indirection, byteorder = 'big'),aggregate_column_index, locations[i][0][0])
        #         if (temp == DELETED): # might be deleted
        #             continue
        #         values  += temp
        #     else:
        #         args = [self.table.name, "Base", aggregate_column_index + NUM_METAS, *page_pointer[0]]
        #         temp = int.from_bytes(BufferPool.get_record(*args), byteorder="big")
        #         if (temp == DELETED): # might be deleted
        #             continue
        #         values += temp
        # return values

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    # TODO : merging -> remove all invalidate record and key in index
    def delete(self, key):
        self.table.delete_count += 1
        #page_pointer = self.table.index.locate(self.table.key,key)
        # null_value = []

        #page_range, page_index, record_index = page_pointer[0],page_pointer[1], page_pointer[2]
        page_pointer = self.table.index.locate(self.table.key,key)
        # for i in range(self.table.num_columns):
        #     null_value.append(DELETED)
        #     self.table.mg_rec_update(NUM_METAS+i, *page_pointer[0])

        update_range_index, update_record_page_index,update_record_index = page_pointer[0][0],page_pointer[0][1], page_pointer[0][2]

        # read from meta-data columnn, indirection
        ops_list = []
        ops_temp = {}
        ops_temp['command_type'] = "delete"
        ops_temp['command_num'] = self.table.delete_count

        ops_temp['column_id'] = INDIRECTION_COLUMN
        ops_temp['r_w'] = "read"
        ops_temp['base_tail'] = "base"
        ops_temp['meta_data'] = "meta"
        ops_temp['page_pointer'] = page_pointer[0]
        ops_temp['page_lacth'] = 0
        ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])
    #    ops_list.append([INDIRECTION_COLUMN, ops_temp])
        #ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])

        # args = [self.table.name, "Base", INDIRECTION_COLUMN, *page_pointer[0]]
        # base_indirection_id = BufferPool.get_record(*args)
        ops_temp = {}
        ops_temp['command_type'] = "delete"
        ops_temp['command_num'] = self.table.delete_count

        ops_temp['column_id'] = RID_COLUMN
        ops_temp['r_w'] = "read"
        ops_temp['base_tail'] = "base"
        ops_temp['meta_data'] = "meta"
        ops_temp['page_pointer'] = page_pointer[0]
        ops_temp['page_lacth'] = 0
        ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])
        #ops_list.append([RID_COLUMN, ops_temp])
        #ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])

        # args = [self.table.name, "Base", RID_COLUMN, *page_pointer[0]]
        # base_rid = BufferPool.get_record(*args)
        # base_id = int.from_bytes(base_rid, byteorder='big')
        ops_temp = {}
        ops_temp['command_type'] = "delete"
        ops_temp['command_num'] = self.table.delete_count

        ops_temp['column_id'] = INDIRECTION_COLUMN
        ops_temp['r_w'] = "read"
        ops_temp['base_tail'] = "tail"
        ops_temp['meta_data'] = "meta"
        ops_temp['page_pointer'] = page_pointer[0]
        ops_temp['page_lacth'] = 1
        ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])
        #ops_list.append([INDIRECTION_COLUMN, ops_temp])
        #ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])

        # tmp_indice = self.table.get_latest_tail(INDIRECTION_COLUMN, update_range_index)
        # args = [self.table.name, "Tail", INDIRECTION_COLUMN, update_range_index, tmp_indice]
        # page_records = BufferPool.get_page(*args).num_records
        # total_records = page_records + tmp_indice*MAX_RECORDS
        # next_tid = total_records
        # next_tid = int.from_bytes(('t'+ str(total_records)).encode(), byteorder = "big")

        # the record is firstly updated

        # if (int.from_bytes(base_indirection_id,byteorder='big') == MAXINT):
        #     # compute new tail record indirection :  the indirection of tail record point backward to base pages
        #     args = [self.table.name, "Base", RID_COLUMN, *page_pointer]
        #     next_tail_indirection = BufferPool.get_record(*args)  # in bytes
        #     next_tail_indirection = int.from_bytes(next_tail_indirection, byteorder='big')
        # else:
        #     next_tail_indirection = int.from_bytes(base_indirection_id,byteorder='big')
        ops_temp = {}
        ops_temp['command_type'] = "delete"
        ops_temp['command_num'] = self.table.delete_count

        ops_temp['column_id'] = SCHEMA_ENCODING_COLUMN
        ops_temp['r_w'] = "read"
        ops_temp['base_tail'] = "base"
        ops_temp['meta_data'] = "meta"
        ops_temp['page_pointer'] = page_pointer[0]
        ops_temp['page_lacth'] = 0
        ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])

        # args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, *page_pointer[0]]
        # encoding_base = BufferPool.get_record(*args)  # in bytes
        # old_encoding = int.from_bytes(encoding_base,byteorder="big")
        # new_encoding = int('1'* self.table.num_columns, 2)
        # schema_encoding = new_encoding
        # starttime = datetime_to_int(datetime.datetime.now())
        # lastupdatetime = 0
        # updatetime = 0
        # update new tail record
        # meta_data = [next_tail_indirection,next_tid,schema_encoding,base_id,starttime,lastupdatetime,updatetime]
        # meta_data.extend(null_value)
        # tail_data = meta_data
        # self.table.tail_page_write(tail_data, update_range_index)

        # overwrite base page with new metadata
        ops_temp = {}
        ops_temp['command_type'] = "delete"
        ops_temp['command_num'] = self.table.delete_count

        ops_temp['column_id'] = INDIRECTION_COLUMN
        ops_temp['r_w'] = "write"
        ops_temp['base_tail'] = "base"
        ops_temp['meta_data'] = "meta"
        ops_temp['page_pointer'] = page_pointer[0]
        ops_temp['page_lacth'] = 0
        ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])

        # args = [self.table.name, "Base", INDIRECTION_COLUMN, page_pointer[0][0], page_pointer[0][1]]
        # page = BufferPool.get_page(*args)
        # page.update(update_record_index, next_tid)
        ops_temp = {}
        ops_temp['command_type'] = "delete"
        ops_temp['command_num'] = self.table.delete_count

        ops_temp['column_id'] = SCHEMA_ENCODING_COLUMN
        ops_temp['r_w'] = "write"
        ops_temp['base_tail'] = "base"
        ops_temp['meta_data'] = "meta"
        ops_temp['page_pointer'] = page_pointer[0]
        ops_temp['page_lacth'] = 0
        ops_list.append([(page_pointer[0][0], page_pointer[0][1]), ops_temp])

        # args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, page_pointer[0][0], page_pointer[0][1]]
        # page = BufferPool.get_page(*args)
        # page.update(update_record_index, schema_encoding)

        # self.table.num_updates += 1
        # self.table.mergeThreadController()
        return ops_list
    #    self.table.invalidate_record(page_range, page_index, record_index)

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

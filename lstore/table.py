from lstore.page import *
from lstore.config import *
from lstore.index import Index
from time import time
import time as t
from lstore.page_range import *
from lstore.buffer_pool import BufferPool
# queue is used for managing threads, thread is defined per column per page range
from queue import Queue
import threading
import multiprocessing as mp
import os
import copy
import time

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.indirection = None

# this is for milestone 3, while each page range in each column will be a thread
# class range_Thread(threading.Thread):
#     def _init_(self, pg_range):
#         threading.Thread._init_(self)
#         self.pg_range = pg_range
#     def run(self):
#         pass


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
        self.index = Index(self)
        self.num_updates = 0
        self.num_records = 0
        self.merge_pid = None
        self.merged_record = {}
        # background merge thread is running as table started


    def __merge(self):
        keys, p_indices = BufferPool.get_table_tails(self.name)
        for (col_index, rg_index), last_p_index in zip(keys, p_indices):
            if col_index < NUM_METAS:
                continue

            args = [self.name, 'Tail', col_index, rg_index, last_p_index]
            last_page = BufferPool.get_page(*args)

            old_tps = BufferPool.get_tps(self.name, col_index, rg_index)
            new_tps = last_p_index*MAX_RECORDS + last_page.num_records

            page_range = BufferPool.get_base_page_range(self.name, col_index, rg_index)
            page_range_copy = copy.deepcopy(page_range)

            merged_record = {}
            for uid in page_range_copy.keys():
                t_name, base_tail, col_id, range_id, page_id = uid
                for rec_id in range(MAX_RECORDS):
                    # merged_record contains all the records within base page range
                    merged_record[(t_name, base_tail, col_id, range_id, page_id, rec_id)] = 0 # Init

            max_merged_count = len(list(self.merged_record.keys()))
            early_stopping = 0
            start_tail_p_index = (new_tps-1)//MAX_RECORDS
            end_tail_p_index = old_tps//MAX_RECORDS
            # print("Merging Column {} Page Range {}".format(col_index, rg_index))
            # print("New Tps {} Old Tps {} Start Tail Page  {} End Tail Page {}".format(new_tps, old_tps, start_tail_p_index, end_tail_p_index))

            for rev_page in reversed(range(end_tail_p_index, start_tail_p_index+1)):  # Rly need to double check, so easily to messed it up
                args_rid = [self.name, 'Tail', BASE_RID, rg_index, rev_page]
                args_data = [self.name, 'Tail', col_index, rg_index, rev_page]

                for rev_rec in reversed(range(0, MAX_RECORDS)):
                    rid = int.from_bytes(BufferPool.get_page(*args_rid).get(rev_rec), byteorder = 'big')
                    base_page, base_rec = rid % (MAX_RECORDS*PAGE_RANGE) // MAX_RECORDS, rid % (MAX_RECORDS*PAGE_RANGE) % MAX_RECORDS
                    uid = (self.name, "Base", col_index, rg_index, base_page)
                    uid_w_record = (self.name, "Base", col_index, rg_index, base_page, base_rec)

                    if merged_record[uid_w_record] == 0:
                        update_val = int.from_bytes(BufferPool.get_page(*args_data).get(rev_rec), byteorder='big')
                        if update_val != MAXINT:
                            page_range_copy[uid].update(base_rec, update_val)
                            # Also reset schema encoding to 0
                            args_schema = [self.name, "Base", SCHEMA_ENCODING_COLUMN, rg_index, base_page]
                            old_encoding = int.from_bytes(BufferPool.get_page(*args_schema).get(base_rec), byteorder="big")
                            old_encoding = bin(old_encoding)[2:].zfill(self.num_columns)
                            new_encoding = old_encoding[:self.num_columns-(col_index-NUM_METAS)-1] + "0" + old_encoding[self.num_columns-(col_index-NUM_METAS):]
                            new_encoding = int(new_encoding, 2) # Convert to int
                            BufferPool.page_directories[tuple(args_schema)].update(base_rec, new_encoding)
                        merged_record[uid_w_record] = 1
                        early_stopping += 1

                    if early_stopping == max_merged_count:
                        print("Early Stopped")
                        break

                if early_stopping == max_merged_count:
                    break

            # Base Page Range updates
            BufferPool.update_base_page_range(page_range_copy)
            # TPS updates
            BufferPool.set_tps(self.name, col_index, rg_index, new_tps)
            self.merged_record = {}

    def mg_rec_update(self, col_index, rg_index, pg_index, rc_index):
        self.merged_record[(self.name, "Base", col_index, rg_index, pg_index, rc_index)] = 0

    def mergeThreadController(self):
        if self.num_updates % MERGE_TRIGGER == 0:
            t = threading.Thread(target=self.__merge())
            t.start()
            t.join()

    def mergeProcessController(self):

        print("thread is running")
        r,w = os.pipe()
        n = os.fork()
        if n > 0:
            os.close(w)
            r = os.fdopen(r,'r')
            while self.merge_pid is None:
                res = r.read()
                if res == "":
                    continue
                self.merge_pid = int(res)
                print("merge_pid", self.merge_pid)
            r.close()
            print("Parent process and id is ", os.getpid())
        else:
            os.close(r)
            w = os.fdopen(w,'w')
            data = "{}".format(os.getpid())
            print("data: ", data)
            w.write(data)
            w.close()
            self.__merge()
            print("Child process and id is: ", os.getpid())
    #    f.start()
        print("thread is finished")

    def get_latest_tail(self, column_id, page_range_id):
        return BufferPool.get_latest_tail(self.name, column_id, page_range_id)

    # want to find physical location of tail record given tid
    # tid : bytesarray
    def get_tail(self, tid, column, range_index):
        #tid_str = str(tid.decode()).split('t')[1]
    #    tid = int(tid_str)
        # return int.from_bytes(self.page_directory["Tail"][column+NUM_METAS][range_index][tid//MAX_RECORDS].get(tid%MAX_RECORDS),byteorder='big')
        args = [self.name, "Tail", column+NUM_METAS, range_index, tid//MAX_RECORDS, tid%MAX_RECORDS]
        return int.from_bytes(BufferPool.get_record(*args), byteorder='big')

    # return the columns of attributes given tail record
    def get_tail_columns(self, tid, range_index):
        columns = []
    #    tid_str = str(tid.decode()).split('t')[1]
    #    tid = int(tid_str)
        for column_id in range(0,self.num_columns):
            # columns.append(int.from_bytes(self.page_directory["Tail"][column_id+NUM_METAS][range_index][tid//MAX_RECORDS].get(tid%MAX_RECORDS),byteorder='big'))
            columns.append(self.get_tail(tid, column_id, range_index))
        return columns


    """ invalidating the record : set bid and tids of this record to 0
    def invalidate_record(self, page_range, page_index, record_index):
        # invalidate the bid
        #for i in range()
        rid_page_range = self.page_directory["Base"][RID_COLUMN]
        rid_page_range[page_range].get_value(page_index).data[record_index*8:(record_index+1)*8] = (0).to_bytes(8, byteorder='big')
        # invalidate the tid
        tid_page_range = self.page_directory["Tail"][RID_COLUMN]
        byte_indirect = self.page_directory["Base"][INDIRECTION_COLUMN][page_range].get_value(page_index).get(record_index)
        while ('b' not in byte_indirect.decode()) & (byte_indirect != MAXINT.to_bytes(8,byteorder = "big")):
            tid_str = str(byte_indirect.decode()).split('t')[1]
            tid = int(tid_str)
            pre_updates = 0
            for i in range(page_range):
                for page in self.page_directory["Tail"][column+NUM_METAS][i]:
                    pre_updates += page.num_records
            in_range_tid = tid - pre_updates
            page_index,record_index = in_range_tid//MAX_RECORDS,in_range_tid%MAX_RECORDS
            tid_page_range[page_range][page_index].data[record_index*8:(record_index+1)*8] = (0).to_bytes(8, byteorder='big')
            byte_indirect = self.page_directory["Tail"][INDIRECTION_COLUMN][page_range][page_index].get(record_index)
    """
    def base_page_write(self, data):
        for i, value in enumerate(data):
            range_index = (self.num_records//MAX_RECORDS)//PAGE_RANGE
            page_index = (self.num_records//MAX_RECORDS) % PAGE_RANGE
            args = [self.name, "Base", i, range_index, page_index]
            # latest base page
            page = BufferPool.get_page(*args)
            # page_range = self.page_directory["Base"][i][-1]
            # page = page_range.page_range[page_range.curr_page]

            # check if page range currently at the end of the page
            if page_index < PAGE_RANGE:
                # Edge Case
                if page_index == 0:
                    t_ages = [self.name, "Tail", i, range_index, page_index]
                    BufferPool.add_page(tuple(t_ages))  # Create new Tail Page
                    BufferPool.set_tps(self.name, i, range_index)
                    BufferPool.set_latest_tail(self.name, t_ages[2], t_ages[3], t_ages[4])

                # Page range not at the end. Verify if Page is full
                if not page.has_capacity():
                    # need a new page allocation
                    args[-1] += 1  # increment page index
                    page = BufferPool.get_page(*args)

                    # self.page_directory["Base"][i][-1].write()
                    # page = self.page_directory["Base"][i][-1].get()
            else:
                # Page is full, need a new page range and new page
                args[-2] += 1  # Increment Page Range
                args[-1] = 0  # Reset Page Index to 0
                page = BufferPool.get_page(*args)  # Create New Base Page Range
                args[1] = "Tail"
                BufferPool.add_page(tuple(args))  # Create new Tail Page
                self.add_latest_tail(args[2], args[3], args[4])

            page.dirty = 1
            page.write(value)


    def tail_page_write(self, data, range_index):
        for i, value in enumerate(data):
            page_id = self.get_latest_tail(i, range_index)
            args = [self.name, "Tail", i, range_index, page_id]
            page = BufferPool.get_page(*args)

            # Verify Page is not full
            if not page.has_capacity():
                args[-1] += 1
                BufferPool.set_latest_tail(self.name, i, range_index, args[-1]) # Update Latest Tail
                page = BufferPool.get_page(*args)

            page.dirty = 1
            page.write(value)

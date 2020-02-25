from lstore.page import *
from lstore.config import *
from lstore.index import Index
from time import time
from lstore.page_range import *
from lstore.buffer_pool import BufferPool
# queue is used for managing threads, thread is defined per column per page range
from queue import Queue
import _thread as thread

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
        # self.page_directory = {}
        #self.index = Index(self) # newly added
        self.num_updates = 0
        self.num_records = 0
        self.index = Index(self)
        self.latest_tail = {}  # Key: tuple(col, range), Value: Page_id
        # self.__init_pages()
        # background merge thread is running as table started
        # thread.start_new_thread(self.__merge())

    def add_latest_tail(self, column_id, page_range_id, page_id):
        # import pdb; pdb.set_trace()
        uid = (int(column_id), int(page_range_id))
        self.latest_tail[uid] = int(page_id)

    def get_latest_tail(self, uid):
        # print(BufferPool.page_directories.keys())
        # import pdb; pdb.set_trace()
        return self.latest_tail[uid]

    # def __init_pages(self):
    #     self.page_directory = {
    #         "Base": {},
    #         "Tail": {}
    #     }

    #     # reinitialize the page directory to accomodate the page range
    #     for i in range(self.num_columns + NUM_METAS):
    #         self.page_directory["Base"][i] = [Page_Range()]
    #         # list of page ranges initializing the first with an empty page
    #         self.page_directory["Tail"][i] = [[Page()]]

    # TODO: Dont Remove this yet
    # def __get_base_loc(self, col, key):
    #     page_ranges = self.page_directory["Base"][col]
    #     for i in range(len(page_ranges)):
    #         for j in range(page_ranges[i].curr_page+1):
    #             for k in range(page_ranges[i].get_value(j).num_records):
    #                 if key == page_ranges[i].get_value(j).get(k):
    #                     return i, j, k
        self.__init_pages()
        # main thread which will control merge thread
        #self.__mergeController()

    def __init_pages(self):
        self.page_directory = {
            "Base": {},
            "Tail": {}
        }

        # reinitialize the page directory to accomodate the page range
        for i in range(self.num_columns + NUM_METAS):
            self.page_directory["Base"][i] = [Page_Range()]
            # list of page ranges initializing the first with an empty page
            self.page_directory["Tail"][i] = [[Page()]]

    def __get_base_loc(self, col, key):
        page_ranges = self.page_directory["Base"][col]
        for i in range(len(page_ranges)):
            for j in range(page_ranges[i].curr_page+1):
                for k in range(page_ranges[i].get_value(j).num_records):
                    if key == page_ranges[i].get_value(j).get(k):
                        return i, j, k
    """
    Step1: Identify committed tail records in tail pages:
    Select a set of consecutive fully committed tail records (or pages) since the
    last marge within each update range

    Step2: Load the corresponding outdated base pages:
    For a selected set of committed tail records, load the corresponding outdated
    base pages for the given update range (limit the load to only outdated columns)

    optimization:
    Avoid to load sub-ranges of records that have not yet changed since the last
    merge.

    Step3: Consolidate the base and tail pages:
    For every updated column, the merge process will read a outdated base pages and
    applies a set of recent committed updates from the tail pages and writes out m
    new pages.

    First the BaseRID column of the committed tail pages are scanned in reverse
    order to find the list of the latest version of every updated record.

    Hashable -> track whether the latest version of a record is seen or not

    Apply update until the base range is seen, skipping any intermediate updates

    Needs special dealings with deleted records

    """
    def __mergeController(self):
        # background merge thread is running as table started
        thread.start_new_thread(self.__merge())


    #         if range_records > MERGE_TRIGGER:
    #             cur_thread = range_Thread(cur_page_range)
    #             self.queueThreads.put(cur_thread)

    def __merge(self):
        # initialize threads for all the page ranges in every column
        # if their number of updates within page range is above 2 physical page
        # Insert selected page range into queue
        self.queueThreads = Queue()
        for i in range(NUM_METAS, NUM_METAS+self.num_columns):
            for rg_index, cur_tail_pages in enumerate(self.page_directory["Tail"][i]):
                range_records = (len(cur_tail_pages)-1)*MAX_RECORDS + cur_tail_pages[len(cur_tail_pages)-1].num_records
                if range_records > MERGE_TRIGGER:
                    self.queueThreads.put([i, rg_index])
        # create a copy of a base batch, optimizing by only loading updated base records
        self.base_dir_copy = self.page_directory["Base"]
        while not self.queueThreads.empty():
            col_index, cur_rg_index = self.queueThreads.get()[0], self.queueThreads.get()[1]
            cur_base_map = self.page_directory["Base"][col_index][cur_rg_index].Hashmap
            mergeSeen = len(cur_base_map)
            # reading a set of tail pages in reverse order
            cur_tail_batch = self.page_directory["Tail"][col_index][cur_rg_index]
            for rev_page in reversed(range(-len(cur_tail_batch), 0)):
                for rev_rec in reversed(range(-rev_page.num_records, 0)):
                    rid = int.from_bytes(self.base_dir_copy[RID_COLUMN][cur_rg_index].get_value(rev_page).get(rev_rec), byteorder = 'big')
                    if cur_base_map[rid] == 1:
                        update_val = cur_tail_batch[rev_page].get(rev_rec)
                        self.base_dir_copy[col_index][cur_rg_index].get_value(rev_page).update(rev_rec, update_val)
                        cur_base_map[rid] = 0
                        mergeSeen -= 1
                    # if all RIDs are seen for updated base records
                    if mergeSeen == 0:
                        break
                if mergeSeen == 0:
                    break
            self.base_dir_copy[col_index][cur_rg_index].TPS = int.from_bytes(self.base_dir_copy[RID_COLUMN][cur_rg_index].get_value(rev_page).get(rev_rec), byteorder = 'big')


    # want to find physical location of tail record given tid
    # tid : bytesarray
    def get_tail(self,tid,column, range_index):
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
                    self.add_latest_tail(t_ages[2], t_ages[3], t_ages[4])

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
            page_id = self.latest_tail[(i, range_index)]
            args = [self.name, "Tail", i, range_index, page_id]
            page = BufferPool.get_page(*args)

            # Verify Page is not full
            if not page.has_capacity():
                args[-1] += 1
                self.latest_tail[(i, range_index)] = args[-1] # Update Latest Tail
                page = BufferPool.get_page(*args)

            page.dirty = 1
            page.write(value)

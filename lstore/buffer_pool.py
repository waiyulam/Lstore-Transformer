from lstore.config import *
from lstore.page import *
import os
import heapq
import pickle
from datetime import datetime
import time
import copy


class BufferPool():

    def __init__(self):
        self.size = BUFFER_POOL_SIZE
        self.path = None

        # active pages loaded in bufferpool
        self.page_directories = {}
        self.tstamp_directories = {}

        # Pop the least freuqently used page
        self.timestamp = []  # [[timestamp1, uid1], [timestamp2, uid2] ...]

    def initial_path(self, path):
        self.path = path

    def add_meta(self, uid):
        self.page_directories[uid] = None
        self.tstamp_directories[uid] = datetime.timestamp(datetime.now())

    def read_page(self, page_path):
        f = open(page_path, "rb")
        page = pickle.load(f)  # Load entire page object
        new_page = Page()
        new_page.from_file(page)
        f.close()
        return new_page

    def write_page(self, page, page_path):
        # Create if not existed
        dirname = os.path.dirname(page_path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        f = open(page_path, "wb")
        f.dump(page)  # Dump entire page object
        f.close()

    def get_page(self, t_name, base_tail, column_id, page_range_id, page_id):
        uid = tuple(t_name, base_tail, column_id, page_range_id, page_id)
        page_path = os.path.join(self.path, t_name, base_tail, column_id, page_range_id, str(page_id) + ".txt")
        page = self.page_directories(uid)

        # Page not loaded in buffer, load from disk
        if page is None:
            # Have Space in bufferbool
            if not self.is_full():
                self.page_directories[uid] = self.read_page(page_path)
                # self.timestamp.append([datetime.timestamp(datetime.now()), uid])
                self.tstamp_directories[uid] = datetime.timestamp(datetime.now())

            # No Space in bufferbool
            else:
                # Pop least recently used page in cache
                # sorted_timestamps = sorted(self.timestamp)
                # oldest_freq, oldest_uid = sorted_timestamps[0] # FIXME: More complex control needed for pinning
                sorted_uids = sorted(self.tstamp_directories, key=self.tstamp_directories.get)
                oldest_uid = sorted_uids # FIXME: More complex control needed for pinning
                oldest_page = self.page_directories[oldest_uid]
                assert(oldest_page is not None)

                # Check if old_page is dirty => write back
                if oldest_page.dirty == 1:
                    t_name, base_tail, column_id, page_range_id, page_id = oldest_uid
                    old_page_path = os.path.join(self.path, t_name, base_tail, column_id, page_range_id, str(page_id) + ".txt")
                    self.write_page(oldest_page, old_page_path)

                # Load target page
                new_page = self.read_page(page_path)

                # Reset States
                self.page_directories[oldest_uid] = None
                self.page_directories[uid] = new_page
                del self.tstamp_directories[oldest_uid]
                self.tstamp_directories[uid] = datetime.timestamp(datetime.now())
                # self.timestamp[oldest_page_id] = [datetime.timestamp(datetime.now()), uid]
            page = new_page
        # Page already in buffer
        else:
            self.tstamp_directories[uid] = datetime.timestamp(datetime.now())
            # self.timestamp[page_id][0] = datetime.timestamp(datetime.now())  # Update timestamp

        return page

    def is_full(self):
        # return len(self.timestamp) >= self.size
        return len(self.tstamp_directories) >= self.size

    def close(self):
        active_uids = self.tstamp_directories.values()
        while len(active_uids) > 0:
            active_uids_copy = copy.deepcopy(active_uids)
            # Loop Through Pages in Bufferpool
            for i, uid in enumerate(active_uids_copy):
                page = self.page_directories[uid]
                # Write Back Dirty Pages
                if page.dirty and not page.pinned:
                    # _, uid = self.timestamp[page_id]
                    t_name, base_tail, column_id, page_range_id, page_id = uid
                    page_path = os.path.join(self.path, t_name, base_tail, column_id, page_range_id, str(page_id) + ".txt")
                    self.write_page(page, page_path)
                    active_uids.pop(uid)
                    # self.pages.pop(page_id)

            # Wait until Pinned Pages are unpinned
            time.sleep(1)
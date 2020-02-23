from lstore.config import *
from lstore.page import *
import os
import heapq
import pickle
from datetime import datetime
import time
import copy


class BufferPool():

    def __init__(self, path):
        self.size = BUFFER_POOL_SIZE
        self.path = None

        # active pages loaded in bufferpool
        self.pages = []

        # A look up table if a page is being used of not
        self.uid_2_pageid = {}  # key: uid, value: page_id (index of self.pages)

        # Pop the least freuqently used page
        self.timestamp = []  # [[timestamp1, uid1], [timestamp2, uid2] ...]

    def init_path(self, path):
        self.path = path

    def initial_meta(self, meta_f, t_name):
        f = open(meta_f, "r")
        lines = f.readlines()
        f.close()

        for line in lines[1:]:
            base_tail, column_id, page_range_id, page_id = line.strip(',')
            uid = tuple(t_name, base_tail, column_id, page_range_id, page_id)
            if uid not in self.uid_2_pageid.keys():
                self.uid_2_pageid[uid] = -1

    # def initial_pool(self):
    #     # Set used Tables
    #     self.used_frequency = heapq.nlargest(self.size, self.all_frequency)
    #     for freq, uid in self.used_frequency:
    #         t_name, base_tail, column_id, page_range_id, page_id = uid
    #         page_path = os.path.join(self.path, t_name, base_tail, column_id, page_range_id, str(page_id) + ".txt")
    #         page = self.read_page(page_path)
    #         # Point to corresponding page index
    #         self.uid_2_pageid[uid] = len(self.pages)
    #         self.pages.append(page)

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
        page_id = self.uid_2_pageid[uid]

        # Page not loaded in buffer, load from disk
        if page_id == -1:
            # Have Space in bufferbool
            if not self.is_full():
                new_page = self.read_page(page_path)
                page_id = len(self.pages)
                self.uid_2_pageid[uid] = page_id
                self.pages.append(new_page)
                self.timestamp.append([datetime.timestamp(datetime.now()), uid])

            # No Space in bufferbool
            else:
                # Pop least recently used page in cache
                sorted_timestamps = sorted(self.timestamp)
                oldest_freq, oldest_uid = sorted_timestamps[0] # FIXME: More complex control needed for pinning
                oldest_page_id = self.uid_2_pageid[oldest_uid]
                assert(oldest_page_id != -1)
                old_page = self.pages[oldest_page_id]

                # Check if old_page is dirty => write back
                if old_page.dirty == 1:
                    t_name, base_tail, column_id, page_range_id, page_id = least_uid
                    old_page_path = os.path.join(self.path, t_name, base_tail, column_id, page_range_id, str(page_id) + ".txt")
                    self.write_page(old_page, old_page_path)

                # Load target page
                new_page = self.read_page(page_path)

                # Reset States
                self.uid_2_pageid[oldest_uid] = -1
                self.uid_2_pageid[uid] = oldest_page_id
                self.pages[oldest_page_id] = new_page
                self.timestamp[oldest_page_id] = [datetime.timestamp(datetime.now()), uid]
            page = new_page
        # Page already in buffer, load from disk
        else:
            page_id = self.uid_2_pageid[uid]
            self.timestamp[page_id][0] = datetime.timestamp(datetime.now()) # Update timestamp
            page = self.pages[page_id]

        return page

    def is_full(self):
        return len(self.pages) >= self.size

    def write_back_all(self):
        while len(self.pages) > 0:
            pages = copy.deepcopy(self.pages)
            # Loop Through Pages in Bufferpool
            for page_id, page in enumerate(pages):
                # Write Back Dirty Pages
                if page.dirty and not page.pinned:
                    _, uid = self.timestamp[page_id]
                    t_name, base_tail, column_id, page_range_id, page_id = uid
                    page_path = os.path.join(self.path, t_name, base_tail, column_id, page_range_id, str(page_id) + ".txt")
                    self.write_page(page, page_path)
                    self.pages.pop(page_id)
            # Wait until Pinned Pages are unpinned
            time.sleep(1)
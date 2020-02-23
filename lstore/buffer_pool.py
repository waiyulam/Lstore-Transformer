from lstore.config import *
from lstore.page import *
import os
import heapq
import pickle
from datetime import datetime
import time
import copy


def read_page(page_path):
    f = open(page_path, "rb")
    page = pickle.load(f)  # Load entire page object
    new_page = Page()
    new_page.from_file(page)
    f.close()
    return new_page


def write_page(page, page_path):
    # Create if not existed
    dirname = os.path.dirname(page_path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    f = open(page_path, "wb")
    f.dump(page)  # Dump entire page object
    f.close()


class BufferPool():
    def __init__(self):
        self.size = BUFFER_POOL_SIZE
        self.path = None

        # active pages loaded in bufferpool
        self.page_directories = {}

        # Pop the least freuqently used page
        self.tstamp_directories = {}

    def initial_path(self, path):
        self.path = path

    def add_page(self, uid):
        self.page_directories[uid] = None

    def is_full(self):
        return len(self.tstamp_directories) >= self.size

    def is_page_in_buffer(self, uid):
        return self.page_directories[uid] is None

    def uid_to_path(self, uid):
        """
        Convert uid to path
        uid: tuple(table_name, base_tail, column_id, page_range_id, page_id)
        """
        t_name, base_tail, column_id, page_range_id, page_id = uid
        path = os.path.join(self.path, t_name, base_tail, column_id,
                            page_range_id, str(page_id) + ".pkl")
        return path

    def get_page(self, t_name, base_tail, column_id, page_range_id, page_id):
        uid = tuple(t_name, base_tail, column_id, page_range_id, page_id)
        page_path = self.uid_to_path(uid)

        # Page not loaded in buffer, load from disk
        if self.is_page_in_buffer(uid):
            # No Space in bufferbool, write LRU page to disk
            if self.is_full():
                # Pop least recently used page in cache
                sorted_uids = sorted(self.tstamp_directories,
                                     key=self.tstamp_directories.get)
                oldest_uid = sorted_uids[0]  # FIXME: More complex control needed for pinning
                oldest_page = self.page_directories[oldest_uid]
                assert(oldest_page is not None)

                # Check if old_page is dirty => write back
                if oldest_page.dirty == 1:
                    old_page_path = self.uid_to_path(oldest_uid)
                    write_page(oldest_page, old_page_path)

                self.page_directories[oldest_uid] = None
                del self.tstamp_directories[oldest_uid]

            self.page_directories[uid] = read_page(page_path)

        self.tstamp_directories[uid] = datetime.timestamp(datetime.now())
        return self.page_directories[uid]

    def close(self):
        active_uids = self.tstamp_directories.values()
        while len(active_uids) > 0:
            active_uids_copy = copy.deepcopy(active_uids)
            # Loop Through Pages in Bufferpool
            for i, uid in enumerate(active_uids_copy):
                page = self.page_directories[uid]
                # Write Back Dirty Pages
                if page.dirty and not page.pinned:
                    page_path = self.uid_to_path(uid)
                    write_page(page, page_path)
                    active_uids.pop(uid)

            # Wait until Pinned Pages are unpinned
            time.sleep(1)

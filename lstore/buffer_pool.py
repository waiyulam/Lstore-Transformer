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
    pickle.dump(page, f)  # Dump entire page object
    f.close()


class BufferPool:
    size = BUFFER_POOL_SIZE
    path = None

    # active pages loaded in bufferpool
    page_directories = {}

    # Pop the least freuqently used page
    tstamp_directories = {}

    def __init__(self):
        print("Init BufferPool. Do Nothing ...")
        pass

    @classmethod
    def initial_path(cls, path):
        cls.path = path

    @classmethod
    def add_page(cls, uid, default=True):
        if default:
            cls.page_directories[uid] = None
        else:
            cls.page_directories[uid] = Page()
            cls.page_directories[uid].dirty = 1

    @classmethod
    def is_full(cls):
        return len(cls.tstamp_directories) >= cls.size

    @classmethod
    def is_page_in_buffer(cls, uid):
        return cls.page_directories[uid] is not None

    @classmethod
    def uid_to_path(cls, uid):
        """
        Convert uid to path
        uid: tuple(table_name, base_tail, column_id, page_range_id, page_id)
        """
        t_name, base_tail, column_id, page_range_id, page_id = uid
        path = os.path.join(cls.path, t_name, base_tail, str(column_id),
                            str(page_range_id), str(page_id) + ".pkl")
        return path

    @classmethod
    def get_page(cls, t_name, base_tail, column_id, page_range_id, page_id):
        uid = (t_name, base_tail, column_id, page_range_id, page_id)
        page_path = cls.uid_to_path(uid)
        # import pdb; pdb.set_trace()

        # Brand New Page => Not on disk
        if not os.path.isfile(page_path):
            if cls.is_full():
                cls.remove_lru_page()
            cls.add_page(uid, default=False)
        # Existed Page
        else:
            # Existed Page not in buffer => Read From Disk
            if not cls.is_page_in_buffer(uid):
                if cls.is_full():
                    cls.remove_lru_page()
                cls.page_directories[uid] = read_page(page_path)

        # # Page not loaded in buffer, load from disk
        # if cls.is_page_in_buffer(uid):
        #     # No Space in bufferbool, write LRU page to disk
        #     if cls.is_full():
        #         cls.remove_lru_page()

        #     if os.path.isfile(page_path):
        #         cls.page_directories[uid] = read_page(page_path)
        #     else:
        #         cls.add_page(uid)
        # import pdb; pdb.set_trace()

        cls.tstamp_directories[uid] = datetime.timestamp(datetime.now())
        return cls.page_directories[uid]

    @classmethod
    def remove_lru_page(cls):
        # Pop least recently used page in cache
        sorted_uids = sorted(cls.tstamp_directories,
                                key=cls.tstamp_directories.get)
        oldest_uid = sorted_uids[0]  # FIXME: More complex control needed for pinning
        oldest_page = cls.page_directories[oldest_uid]
        assert(oldest_page is not None)

        # Check if old_page is dirty => write back
        if oldest_page.dirty == 1:
            old_page_path = cls.uid_to_path(oldest_uid)
            write_page(oldest_page, old_page_path)

        cls.page_directories[oldest_uid] = None
        del cls.tstamp_directories[oldest_uid]

    @classmethod
    def get_record(cls, t_name, base_tail, column_id, page_range_id, page_id, record_id):
        page = cls.get_page(t_name, base_tail, column_id, page_range_id, page_id)
        return page.get(record_id)

    @classmethod
    def close(cls):
        active_uids = list(cls.tstamp_directories.keys())
        # import pdb; pdb.set_trace()
        while len(active_uids) > 0:
            active_uids_copy = copy.deepcopy(active_uids)
            # Loop Through Pages in Bufferpool
            for i, uid in enumerate(active_uids_copy):
                page = cls.page_directories[uid]
                # Write Back Dirty Pages
                if page.dirty and not page.pinned:
                    page_path = cls.uid_to_path(uid)
                    write_page(page, page_path)
                    active_uids.pop(active_uids.index(uid)) # TODO: Can be faster easily

            # Wait until Pinned Pages are unpinned
            time.sleep(1)

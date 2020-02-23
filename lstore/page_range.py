from lstore.config import *
from lstore.page import *

class Page_Range:

    def __init__(self):
        self.curr_page = 0
        temp = [None] * PAGE_RANGE
        temp[0] = Page()
        self.page_range = temp
        self.TPS = 0
        self.Hashmap = {}

    def end_range(self):
        num_records = self.curr_page * MAX_RECORDS + self.page_range[self.curr_page].num_records
        return num_records == PAGE_RANGE * MAX_RECORDS

    def write(self):
        self.page_range[self.curr_page+1] = Page()
        self.curr_page += 1

    def get(self):
        return self.page_range[self.curr_page]

    def get_value(self, key):
        return self.page_range[key]

    def Hash_insert(self, rid):
        self.Hashmap[rid] = 1

from lstore.config import *
from lstore.page import *

class Page_Range:

    def __init__(self):
        self.curr_page = 0
        temp = [None] * PAGE_RANGE
        temp[0] = Page()
        self.page_range = temp

    def end_page(self):
        return self.curr_page == PAGE_RANGE

    def write(self):
        self.page_range[self.curr_page+1] = Page()
        self.curr_page += 1

    def get(self):
        return self.page_range[self.curr_page]

    def get_value(self, key):
        return self.page_range[key]

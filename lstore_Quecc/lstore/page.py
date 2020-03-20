from lstore.config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.frequency = 0
        self.dirty = 0
        self.pinned = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < MAX_RECORDS

    """
    :param value: int  #
    """
    def write(self, value):
        while(self.pinned == 1):
            continue
        self.pinned = 1
        self.data[self.num_records * 8 : (self.num_records+1) * 8] = (value).to_bytes(8, byteorder='big')
        self.pinned = 0
        self.num_records += 1

    def get(self, index):
        while(self.pinned == 1):
            continue
        self.pinned = 1
        data = self.data[index*8: (index+1)*8]
        self.pinned = 0
        return data

    def update(self, index, value):
        while(self.pinned == 1):
            continue
        self.dirty = 1
        self.pinned = 1
        self.data[index * 8 : (index+1) * 8] = (value).to_bytes(8, byteorder='big')
        self.pinned = 0

    def from_file(self, page):
        self.num_records = page.num_records
        self.data = page.data

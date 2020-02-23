from lstore.config import *
from lstore.page import *

class buffer_pool:

    def __init__(self):
        self.size = BUFFER_POOL_SIZE
        
        # active pages loaded in bufferpool
        self.pages = []

        # A look up table if a page is being used of not
        self.cached_lookup_table = {}

        # Pop the least freuqently used page
        self.used_frequency = [] # heap

        # Pop the most freuqently used page
        # self.unused_frequency = []




    # def set_cached_dict(self, ):
        '''
            Cache_dict 
            [table][Base / Tail][Column_id][Page_Range_id][Page_id]
            
            'in_buffer': 0, 1
            'id': (Base / Tail, Column_id, Page_Range_id, Page_id)
            # index of self.pages
        '''
        # pass

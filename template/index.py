from template.config import *
from BTrees.OOBTree import OOBTree
 
from functools import reduce
from operator import add
import re
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        # self.indices = [None] *  self.table.num_columns
        rid_col = self.table.get_column(RID_COLUMN)
        self.indices = []
        for col_index in range(self.table.num_columns):
            tree = OOBTree()
            # get the column based on column value
            column = self.table.get_column(3+col_index)
            col_dict = {}
            # print(column)
            for i, byte_arr in enumerate(rid_col):
                index_col = int.from_bytes(column[i], byteorder="big")
                print(index_col)
                if index_col in col_dict:
                    col_dict[index_col].append(byte_arr)
                else:
                    col_dict[index_col] = [byte_arr]
            # print(col_dict)
            tree.update(col_dict)
            self.indices.append(tree)


    # returns the location of all records with the given value on column "column", list of RID locations

    def locate(self, column, value):
        rids = list(self.indices[column].values(min=value, max=value+1, excludemax=True))
        # locations include page index and record index within the page
        locs = []
        if len(rids) > 0:
            for rid in rids[0]:
                str_rid = rid.decode()
                if str_rid.find('b') != -1:
                    str_num = str(str_rid).split('b')[1]
                else:
                    str_num = str(str_rid).split('t')[1]
                loc_rid = int(str_num)
                locs.append([loc_rid//MAX_RECORDS, loc_rid%MAX_RECORDS])
        return locs

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if begin > end:
            return list(self.indices[column].values(min=end, max=begin))[::-1]
        else:
            return list(self.indices[column].values(min=begin, max=end))
        
    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        tree = self.indices[column_number]
        keys = self.table.get_old_column(3) # Primary Key
        keys = [int.from_bytes(key, byteorder="big") for key in keys]
        datas = self.table.get_old_column(column_number + 3)
        datas = [int.from_bytes(data, byteorder="big") for data in datas]

        col_dict = {}
        for key, data in zip(keys, datas):
            if key in col_dict.keys():
                col_dict[key].append(data)
            else:
                col_dict[key] = [data]

        tree.update(col_dict) # key (), value
        self.indices[column_number] = tree

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
from template.config import *
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from BTrees.OOBTree import OOBTree

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
                #print(index_col)
                if index_col in col_dict:
                    col_dict[index_col].append(byte_arr)
                else:
                    col_dict[index_col] = [byte_arr]
            #print(col_dict)
            tree.update(col_dict)
            self.indices.append(tree)

    """
    # returns the location of all records with the given value on column "column", list of RIDs
    """

    def locate(self, column, value):
        return list(self.indices[column].values(min=value, max=value+1, excludemax=True))

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass

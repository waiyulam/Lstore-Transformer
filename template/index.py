from template.table import Table

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table):
        pass

    """
    # returns the location of all records with the given value
    """

    def locate(self, value):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, table, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, table, column_number):
        pass

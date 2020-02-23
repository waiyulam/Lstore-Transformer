from lstore.table import Table
from lstore.buffer_pool import BufferPool
import os


class Database():

    def __init__(self):
        self.tables = []
        self.buffer_pool = BufferPool()

    def open(self, path):
        self.buffer_pool.initial_path(path)
        tables = os.listdir(path)
        meta_files = []
        for table in tables:
            meta_files.append(os.path.join(path, table, "config.txt"))

        for meta_f in meta_files:
            # Load in Table() meta data
            f = open(meta_f, "r")
            lines = f.readlines()
            t_name, num_columns, key, num_updates, num_records = lines[0].strip(',')
            old_table = Table(t_name, int(num_columns), int(key))
            old_table.num_updates = int(num_updates)
            old_table.num_records = int(num_records)
            f.close()

            self.tables.append(old_table)
            # self.buffer_pool.add_meta(t_name, lines[1:])

            for line in lines[1:]:
                base_tail, column_id, page_range_id, page_id = line.strip(',')
                uid = tuple(t_name, base_tail, int(column_id), int(page_range_id), int(page_id))

    def close(self):
        self.buffer_pool.close()

        # Write Table Config file
        for table in self.tables:
            t_name = table.name
            f = open(os.path.join(self.buffer_pool.path, t_name, "config.txt", "w"))

            my_list = [table.num_columns, table.key, table.num_updates, table.num_records]
            line = ','.join(my_list) + "\n"
            f.write(line)

            uids = self.buffer_pool.uid_2_pageid[t_name].keys()
            for uid in uids:
                base_tail, column_id, page_range_id, page_id = uid
                my_list = [base_tail, column_id, page_range_id, page_id]
                line = ",".join(my_list) + "\n"
                f.write(line)
            f.close()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        # create a new table in database
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables:
            self.tables.remove(name)
        else:
            pass

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if (table.name == name):
                return table
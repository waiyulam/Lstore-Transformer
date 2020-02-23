from lstore.table import Table
from lstore.buffer_pool import BufferPool
import os


class Database():

    def __init__(self):
        self.tables = []

    def open(self, path):
        BufferPool.initial_path(path)

        # Restore Existed Table on Disk
        tables = os.listdir(path)
        for table in tables:
            f = open(os.path.join(path, table, "table.txt"), "r")
            lines = f.readlines()
            t_name, num_columns, key, num_updates, num_records = lines[0].strip(',')
            old_table = Table(t_name, int(num_columns), int(key))
            old_table.num_updates = int(num_updates)
            old_table.num_records = int(num_records)
            f.close()
            self.tables.append(old_table)

        # Restore Page Directory to BufferPool
        lines = f.readlines()
        f = open(os.path.join(path, "page_directory.txt"), "r")
        for line in lines:
            t_name, base_tail, column_id, page_range_id, page_id = line.strip(',')
            uid = tuple(t_name, base_tail, int(column_id), int(page_range_id), int(page_id))
            BufferPool.add_page(uid)
        f.close()

    def close(self):
        BufferPool.close()

        # Write Table Config file
        for table in self.tables:
            t_name = table.name
            f = open(os.path.join(BufferPool.path, t_name, "table.txt"), "w")
            my_list = [t_name, table.num_columns, table.key, table.num_updates, table.num_records]
            line = ','.join(my_list) + "\n"
            f.write(line)
            f.close()

        # Write Page Directory Config file
        all_uids = BufferPool.page_directories.keys()
        f = open(os.path.join(BufferPool.path, "page_directory.txt"), "w")
        for uid in all_uids:
            t_name, base_tail, column_id, page_range_id, page_id = uid
            my_list = [t_name, base_tail, int(column_id), int(page_range_id), int(page_id)]
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
from lstore.table import Table
from lstore.buffer_pool import BufferPool
import os
import time

# TODO: Write & Reaed latest_tail of each table to disk

class Database():

    def __init__(self):
        self.tables = []

    def open(self, path):
        print("BufferPool Path @ {}".format(path))
        if not os.path.exists(path):
            os.makedirs(path)

        BufferPool.initial_path(path)

        name2idx= {}
        # Restore Existed Table on Disk
        tables = [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]
        for table in tables:
            f = open(os.path.join(path, table, "table.txt"), "r")
            lines = f.readlines()
            # print(lines[0])
            t_name, num_columns, key, num_updates, num_records = lines[0].rstrip('\n').split(',')
            old_table = Table(t_name, int(num_columns), int(key))
            old_table.num_updates = int(num_updates)
            old_table.num_records = int(num_records)
            import pdb; pdb.set_trace()

            f.close()
            name2idx[t_name] = len(self.tables)
            self.tables.append(old_table)

        # Restore Page Directory to BufferPool
        fname = os.path.join(path, "page_directory.txt")
        # Create page_directory.txt if not exist
        if not os.path.exists(fname):
            f = open(fname, "w+")
            f.close()
        f = open(fname, "r")
        lines = f.readlines()
        prev_li = [None] * 5
        for line in lines:
            t_name, base_tail, column_id, page_range_id, page_id = line.rstrip('\n').split(',')
            uid = (t_name, base_tail, int(column_id), int(page_range_id), int(page_id))
            BufferPool.add_page(uid)

            if prev_li[-2] != page_range_id and prev_li[1] == "Tail":
                self.tables[name2idx[t_name]].add_latest_tail(column_id, page_range_id, page_id)
            prev_li = [t_name, base_tail, column_id, page_range_id, page_id]
        f.close()


    def close(self):
        s_time = time.time()
        BufferPool.close()
        print("Closing BufferPool took: {}".format(time.time() - s_time))


        s_time = time.time()
        # Write Table Config file
        for table in self.tables:
            t_name = table.name
            # fname = os.path.join(BufferPool.path, t_name, "table.txt")
            f = open(os.path.join(BufferPool.path, t_name, "table.txt"), "w+")
            my_list = [t_name, str(table.num_columns), str(table.key), str(table.num_updates), str(table.num_records)]
            line = ','.join(my_list) + "\n"
            import pdb; pdb.set_trace()
            f.write(line)
            f.close()
        print("Updating table.txt: {}".format(time.time() - s_time))


        s_time = time.time()
        # Write Page Directory Config file
        all_uids = BufferPool.page_directories.keys()
        f = open(os.path.join(BufferPool.path, "page_directory.txt"), "w")
        for uid in all_uids:
            t_name, base_tail, column_id, page_range_id, page_id = uid
            my_list = [t_name, base_tail, str(column_id), str(page_range_id), str(page_id)]
            line = ",".join(my_list) + "\n"
            f.write(line)
        f.close()
        print("Updating page_directory.txt: {}".format(time.time() - s_time))


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
from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = []
        self.tables_name = []
        self.buffer_pool = BufferPool()

    def open(self, path):
        """
        """
        self.buffer_pool.init_path(path)

        tables = os.listdir(path)
        meta_files = []
        for table in tables:
            meta_files.append(os.path.join(path, table, "config.txt"))
            self.tables_name.append(table)
        
        for name, meta_f in zip(self.tables_name, meta_files):
            # Load in Table() meta data
            f = open(meta_f, "r")
            name, num_columns, key, num_updates, num_records = f.readline()
            old_table = Table(name, num_columns, key)
            old_table.num_updates = num_updates
            old_table.num_records = num_records
            f.close()

            self.buffer_pool.initial_meta(meta_f, name)                

    def close(self):
        self.buffer_pool.write_back_all()

        # Write Table Config file
        for table in self.tables:
            t_name = table.name
            f = open(os.path.join(self.buffer_pool.path, t_name, "config.txt", "w")
            
            my_list = [t_name, table.num_columns, table.key, table.num_updates, table.num_records]
            line =  ','.join(my_list) + "\n"
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
from lstore.table_quecc import Table, Record
from lstore.index import Index
from lstore.buffer_pool import BufferPool
from lstore.config import *
from collections import deque

class TransactionWorker:
    """
    # Adopted algorithm for transaction worker and transactions
    # From the tester, each transaction worker holds muliple transactions
    # At the planning stages,
    # Each transaction holds the plan for its corresponding priority queue
    # Priority queue location will be passd out by indices of transaction workers
    # which corresponds to the specific priority queue
    # At the executon stages,
    # Each transaction worker will load transactions inside priority queue in order
    """

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions, table, transaction_queue_index):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        self.table = table
        self.puzzle = {}
        self.tail_indirections = {}
        self.transaction_queue_index = transaction_queue_index
        pass

    def add_transaction(self, t):
        self.transactions.append(t)

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    # transaction_worker = TransactionWorker([t])
    """
    # current thread is getting ready to execute operations inside one transaction
    # def execution_stage(self):
    """
    def query_select(self, query, page_pointers):
        for page_pointer in page_pointers:
            self.read_base_column(query, page_pointer, )
    """
    def run(self):
        self.planning_stage()
        self.execution_stage()
         # for transaction in self.transactions:
         #     # each transaction returns True if committed or False if aborted
         #     self.stats.append(transaction.planning_stage())
         # # stores the number of transactions that committed
         # self.result = len(list(filter(lambda x: x, self.stats)))


    """
    #op_temp: query_columns, r_w, base_tail, meta_data, rec_location, page_lacth
    """
    def planning_stage(self):
        for transaction in self.transactions:
            r_w_ops_list = transaction.planning_stage()
            # print("list: ", r_w_ops_list)
            for index, r_w_ops in enumerate(r_w_ops_list):
                for op_index, op in enumerate(r_w_ops):
                    if op[0] not in self.table.priority_queues[transaction.queue_idx].keys():
                        self.table.priority_queues[transaction.queue_idx][op[0]] = []
                # locate the priority queue
                    self.table.priority_queues[transaction.queue_idx][op[0]].append(op[1])


    """
    #get op command_type + command_num
    #if read values, store in self.puzzle -> dictionary key: (command_type, command_num, base_tail, op['column_id'], page_pointer), value[column_num, data]
    #if write values, check data in either dictionary or operation, then write the value

    """

    def execution_stage(self):
        # print("check")
        # print(self.table.priority_queues)
        priority_queue = self.table.priority_queues[self.transaction_queue_index]
        print("this is priority queue: ", self.transaction_queue_index)
        for (page_range_id, page_id), queue in priority_queue.items():
            op_queue = deque(queue)
            while op_queue:
                op = op_queue.popleft()
                print(op)
                command_type = op['command_type']
                command_num = op['command_num']
                col_num = op['column_id']

                # if (last_c_num != command_num or last_c_type != command_type or last_col_num != col_num) and last_col_num > NUM_METAS:
                #     for key, value in self.puzzle.items():
                #         keyl = list(key)
                #         if keyl[0] == "sum":
                #             if keyl[0] == last_c_type and keyl[1] == last_c_num and key[3] == last_col_num:
                #                 sum_value += self.puzzle[key]
                #     print("sum ", last_col_num, " is ", sum_value)
                #     sum_value = 0
                if op['r_w'] == "read":
                    if op['base_tail'] == "base":
                        temp = self.read_base_data_column(op['page_pointer'], op['column_id'])
                        key_args = tuple([command_type, command_num, "base", op['column_id'], tuple(op['page_pointer'])])
                        self.puzzle[key_args] = temp
                    #TODO: fix read from tail
                    else:
                        args = tuple([command_type, command_num, "base", INDIRECTION_COLUMN,  tuple(op['page_pointer'])])
                        base_indirection = self.puzzle[args]

                        while base_indirection != MAXINT:
                            # print(base_indirection)
                            # print(command_type, "tail", INDIRECTION_COLUMN, tuple(op['page_pointer']))
                            base_indirection = self.tail_indirections[command_type, "tail", INDIRECTION_COLUMN, tuple(op['page_pointer'])]

                        #     base_indirection = self.puzzle[command_type, command_num, "base", INDIRECTION_COLUMN,  tuple(op['page_pointer'])]

                        temp = self.read_tail_data_column(op['page_pointer'], op['column_id'], base_indirection)
                        key_args = tuple([command_type, command_num, "tail", op['column_id'], tuple(op['page_pointer'])])
                        # print(key_args)
                        self.puzzle[key_args] = temp
                        self.tail_indirections[command_type, "tail", INDIRECTION_COLUMN, tuple(op['page_pointer'])] = MAXINT
                        # print(command_type, "tail", INDIRECTION_COLUMN, tuple(op['page_pointer']),
                        #       self.tail_indirections[command_type, "tail", INDIRECTION_COLUMN, tuple(op['page_pointer'])])
                else:
                    if op['base_tail'] == "base":
                        if op['column_id'] == RID_COLUMN:
                            args = tuple([command_type, command_num, "tail", op['column_id'], tuple(op['page_pointer'])])
                            self.write_base(op['page_pointer'], op['column_id'], self.puzzle[args])
                        else:
                            args = tuple([command_type, command_num, "base", op['column_id'], tuple(op['page_pointer'])])
                            old_schema = self.puzzle[args]
                            if command_type == "update":
                                new_schema = old_schema | (1 << op['operation_column'])
                            else:
                                new_schema = "1" * (self.table.num_columns + NUM_METAS)
                            self.write_base(op['page_pointer'], op['column_id'], new_schema)
                    else:
                        if op['write_data'] != None:
                            self.write_tail(op['page_pointer'], op['column_id'], op['write_data'])
                        else:
                            args = tuple([command_type, command_num, "base", op['column_id'], tuple(op['page_pointer'])])
                            self.write_tail(op['page_pointer'], op['column_id'], self.puzzle[args])


                #construct sum value:
        result = {}
        for key, value in self.puzzle.items():
            keyl = list(key)
            command = tuple([keyl[0], keyl[1], keyl[2], keyl[3]])
            if keyl[0] == "sum":
                if (keyl[3] >= NUM_METAS):
                    if command not in result.keys():
                        result[command] = 0
                    temp = result[command]
                    temp += value
                    result[command] = temp
            elif key[0] == "select":
                result[command] = value
        # print(self.puzzle)
        print("===========================result===========================")
        print(result)


    # read data column from page pointer for specific query column, return specific value of record
    def read_base_data_column(self, page_pointer, column_id):
        args = [self.table.name, "Base", column_id, *page_pointer]
        return int.from_bytes(BufferPool.get_record(*args), byteorder = "big")

    def read_tail_data_column(self, page_pointer, column_id, base_indirection):
        return(self.table.get_tail(base_indirection, column_id, page_pointer[0]))

    # write to one tail record to the tail page
    def write_base(self, page_pointer, column_id, data):
        args = [self.table.name, "Base", column_id, page_pointer[0], page_pointer[1]]
        page = BufferPool.get_page(*args)
        page.update(page_pointer[2], data)

    def write_tail(self, page_pointer, column_id, data):
        self.table.mg_rec_update(column_id, *page_pointer)
        #self.table.tail_page_write(new_rec, page_pointer[0])
        self.table.tail_column_write(data, column_id, page_pointer[0])

#need to sperate the base condition and tail condition
        # update base page indirection and schema encoding
    #    self.table.num_updates += 1

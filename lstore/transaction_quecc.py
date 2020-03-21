from lstore.table_quecc import Table, Record
from lstore.buffer_pool import BufferPool
from lstore.index import Index
from lstore.config import *

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self, queue_idx):
        self.queries = []
        self.queue_idx = queue_idx
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    # r_w_ops = page_pointer,op_temp
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    # current thread is getting ready to plan operations inside one transaction
    def planning_stage(self):
        temp = []
        for query, args in self.queries:
            r_w_ops_list = query(*args)
            temp.append(r_w_ops_list)
        return temp
            # for r_w_ops in r_w_ops_list:
            #     if r_w_ops[0] not in query.table.priority_queues[self.queue_idx]:
            #         query.table.priority_queues[self.queue_idx][r_w_ops[0]] = []
            #     # locate the priority queue
            #     query.table.priority_queues[self.queue_idx][r_w_ops[0]].append(r_w_ops[1])

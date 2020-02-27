from lstore.table import Table, Record
from lstore.index import Index


class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self):
        self.transactions = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    # txn_worker = TransactionWorker([t])
    # th1 = threading.Thread(target=txn_worker.run)
    """

    def run(self):
        for txn in self.transactions:
            txn.run()
        pass


from lstore_2pl.table import Table, Record
from lstore_2pl.index import Index
from lstore_2pl.buffer_pool import BufferPool
from lstore_2pl.config import *
from lstore_2pl.lock_manager import rwlock_manager


class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        # self.table = table
        self.queries = []
        self.locks = dict()
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        # Pre checking : Get locks before executing ops
        for query, args in self.queries:
            page_pointer = query.__self__.table.index.locate(query.__self__.table.key,args[0])
            args = [query.__self__.table.name, "Base",RID_COLUMN, *page_pointer[0]]
            base_rid = int.from_bytes(BufferPool.get_record(*args), byteorder='big')
            # Check if transaction has lock at this record A 
            q_lock = self.locks.get(base_rid,None)
            # Tester only test two query method : select == read increment == write 
            if query == query.__self__.select:
                # require read lock 
                if q_lock == None: 
                    # No lock on this record 
                    if query.__self__.table.rwlock_manager.acquire_reader(base_rid) == False:
                        return self.abort()
                    else:
                        self.locks[base_rid] = 'reader'
            elif query == query.__self__.increment: 
                # require write lock 
                if q_lock == None:
                    if query.__self__.table.rwlock_manager.acquire_writer(base_rid) == False:
                        return self.abort()
                    else:
                        self.locks[base_rid] = 'writer'
                elif q_lock == 'reader':
                    query.__self__.table.rwlock_manager.release_reader(base_rid)
                    if query.__self__.table.rwlock_manager.acquire_writer(base_rid) == False:
                        return self.abort()
                    else:
                        self.locks[base_rid] = 'writer'

        # Safe to execute all ops : no need to do rollback 
        for query,args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()

        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        for i, (rid, l) in enumerate(self.locks.items()):
            if l == 'reader':
                self.queries[0][0].__self__.table.rwlock_manager.release_reader(rid)
            elif l == 'writer':
                self.queries[0][0].__self__.table.rwlock_manager.release_writer(rid)
        return False

    def commit(self):
        # TODO: commit to database ( durability : write to disk )
        for i, (rid, l) in enumerate(self.locks.items()):
            if l == 'reader':
                self.queries[0][0].__self__.table.rwlock_manager.release_reader(rid)
            elif l == 'writer':
                self.queries[0][0].__self__.table.rwlock_manager.release_writer(rid)
        return True


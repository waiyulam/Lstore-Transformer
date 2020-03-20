from collections import defaultdict
import threading

class rwlock_manager:
    def __init__(self):
        # locks stores a hashtable indicate the lock management for each record 
        self.locks = defaultdict(ReadWriteLock)
        pass 

    def acquire_reader(self,rid):
        return self.locks[rid].acquire_read()

    def release_reader(self,rid):
        return self.locks[rid].release_read()
    
    def acquire_writer(self,rid):
        return self.locks[rid].acquire_write()
    
    def release_writer(self,rid):
        return self.locks[rid].release_write()


class ReadWriteLock:
    """ A lock object that allows many simultaneous "read locks", but only one exclusive "write lock." 
        * The no-wait policy avoid deadlock 
        TODO: avoid writer starvation 
        TODO: implement wait policy but do deadlock prevention 
    """

    def __init__(self):
        # Avoid race condition on reader and writer counter 
        self._rw_ready = threading.Lock()
        # counts the number of readers who are currently in the read-write lock (initially zero)
        self._readers = 0
        self._writers = False

    def acquire_read(self):
        """ Acquire a read lock. return false (abort) only if a thread has acquired the write lock. """
        self._rw_ready.acquire()
        if self._writers:
            self._rw_ready.release()
            return False
        else:
            self._readers += 1
            self._rw_ready.release()
            return True

    def release_read(self):
        """ Release a read lock. """
        self._rw_ready.acquire()
        try:
            self._readers -= 1
        finally:
            self._rw_ready.release()

    def acquire_write(self):
        """ Acquire a write lock.  """
        self._rw_ready.acquire()
        if self._readers != 0 :
            self._rw_ready.release()
            return False
        elif self._writers: 
            self._rw_ready.release()
            return False
        else:   
            self._writers = True
            self._rw_ready.release()
            return True

    def release_write(self):
        """ Release a write lock. """
        self._rw_ready.acquire()
        try:
            self._writers = False 
        finally:
            self._rw_ready.release()
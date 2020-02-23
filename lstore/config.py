# Global Setting for the Database
# PageSize, StartRID, etc..

<<<<<<< HEAD
MAX_RECORDS = 512 # 4096/8
MERGE_TRIGGER = 2*MAX_RECORDS # number of updates within page range
# MAX_COLUMNS = 64
=======
# MAX_COLUMNS = 64

# special value
MAXINT = 2**64 - 1

>>>>>>> 790668ec8059d52c5cc8de170dfa9c06bf1a4d1b
# fixed parameter
MAX_RECORDS = 512 # 4096/8
# defined parameters
PAGE_RANGE = 16
<<<<<<< HEAD
=======
MERGE_TRIGGER = 2*MAX_RECORDS # number of updates within page range

>>>>>>> 790668ec8059d52c5cc8de170dfa9c06bf1a4d1b
# meta_data
NUM_METAS = 7
BUFFER_POOL_SIZE = 100
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 2
BASE_RID = 3
STARTTIME_COLUMN = 4
LASTUPDATETIME_COLUMN = 5
UPDATETIME_COLUMN = 6

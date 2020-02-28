# Global Setting for the Database
# PageSize, StartRID, etc..

# MAX_COLUMNS = 64

# special value
RECURSION_LIMIT = 100000
MAXINT = 2**64 - 1
DELETED = 2**64 - 2

# fixed parameter
MAX_RECORDS = 512 # 4096/8

# defined parameters
PAGE_RANGE = 16
MERGE_TRIGGER = MAX_RECORDS # integer number of page size
BUFFER_POOL_SIZE = 100

# meta_data
NUM_METAS = 7
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 2
BASE_RID = 3
STARTTIME_COLUMN = 4
LASTUPDATETIME_COLUMN = 5
UPDATETIME_COLUMN = 6

PAGE_SIZE = 4096
RECORD_SIZE = 128 // 8
MAX_BUFFER_PAGES = None   # Maximum number of pages in buffer (None -> uncapped)
PRINT_ERRORS = True
DEBUG_PRINT = True
UID_DIR = "db_storage/"
MERGE_UPDATE_THRESHOLD = 1_000  # Number of updates to trigger merge
MERGE_BATCH_SIZE = 2              # Number of base pages processed per batch
# USE_MRU_NOT_LRU = False # Whether to use LRU or MRU cache eviction

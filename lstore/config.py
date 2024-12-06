PAGE_SIZE = 4096
RECORD_SIZE = 128 // 8
MAX_BUFFER_PAGES = None   # Maximum number of pages in buffer (None -> uncapped)
PRINT_ERRORS = True
DEBUG_PRINT = False
UID_DIR = "db_storage/"
MERGE_UPDATE_THRESHOLD = 100_000  # Number of updates to trigger merge
MERGE_BATCH_SIZE = 1_000      # Number of base pages processed per batch

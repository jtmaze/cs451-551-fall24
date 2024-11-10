PAGE_SIZE = 4096
RECORD_SIZE = 256
MAX_BUFFER_PAGES = None   # Maximum number of pages in buffer (None -> uncapped)
CUMULATIVE_UPDATE = True # Noncumulative currently not implemented yet, leave as True
PRINT_ERRORS = True
DEBUG_PRINT = True
WRITE_QUEUE_ROWS = 256  # Number of max data records in write queue
WRITE_QUEUE_COLS = 5    # Number of updates allowed in write queue

DB_NAME = "crawl"
NUM_WORKERS = 5

CRAWL_QUEUE_MAX_SIZE = 3 * NUM_WORKERS
CRAWL_QUEUE_LOAD_FACTOR = 0.2
CRAWL_QUEUE_MIN_SECONDS = 5
CRAWL_QUEUE_MAX_SECONDS = 20

NEW_LINKS_QUEUE_MAX_SIZE = 3 * NUM_WORKERS
NEW_LINKS_QUEUE_LOAD_FACTOR = 0.7
NEW_LINKS_QUEUE_MAX_WAIT_SECONDS = 10

COMPLETED_QUEUE_MAX_SIZE = 3 * NUM_WORKERS
COMPLETED_QUEUE_LOAD_FACTOR = 0.7
COMPLETED_QUEUE_MAX_WAIT_SECONDS = 10


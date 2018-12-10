import datetime
import logging
import time
from collections import defaultdict
from threading import Thread
from Queue import Queue, Empty

from ariados.common import constants
from ariados.common import stats
from ariados.handlermanager import HandlerManager
from ariados.utils import run_forever

from .store import JsonStore
from .cockroach import Cockroach
from .worker import Worker
from .invokers import AWSLambdaInvoker
from .pgdb import Status, PostgresDB

logger = logging.getLogger(__name__)

class Context(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

class Master(object):
    def __init__(self):
        self.threads = []
        self.invoker_factory = AWSLambdaInvoker
        self.hm = HandlerManager()

        self.init_queues()
        self.init_store()
        self.init_workers()
        self.init_service_threads()
        self.init_gauges()

    def init_gauges(self):
        stats.client.gauge("urls.waiting.in_mem", 0)

        for name in dir(constants):
            if name.startswith("_"):
                continue

            if not name.isupper():
                continue

            value = getattr(constants, name)
            if not isinstance(value, (int, float)):
                continue

            stat_name = name.lower()
            stats.client.gauge('constants.%s' % stat_name, value)

    def init_queues(self):
        self.crawl_queue = Queue(maxsize=constants.CRAWL_QUEUE_MAX_SIZE)
        self.insert_into_db_queue = Queue(maxsize=constants.INSERT_INTO_DB_QUEUE_MAX_SIZE)
        self.update_db_queue = Queue(maxsize=constants.UPDATE_DB_QUEUE_MAX_SIZE)
        self.store_queue = Queue(maxsize=constants.STORE_QUEUE_MAX_SIZE)
        self.batch_queue = Queue(maxsize=constants.BATCH_QUEUE_MAX_SIZE)

    def init_store(self):
        self.store = JsonStore()

    def init_workers(self):
        self.workers = []
        for i in xrange(constants.NUM_WORKERS):
            w = Worker(self.invoker_factory, self.batch_queue,
                self.insert_into_db_queue, self.update_db_queue, self.store_queue)

            w.daemon = True
            self.workers.append(w)
            self.threads.append(w)
            w.start()

    def init_service_threads(self):
        last_run = datetime.datetime.now() - datetime.timedelta(days=1)

        # NOTE will having multiple connection objects be faster?
        db = PostgresDB(constants.DB_NAME)

        # inserts from disk into in memory crawlqueue
        ctx = Context(db=db, last_run=last_run, last_successful_run=last_run)
        self.crawl_queue_inserter_thread = Thread(
            target=run_forever(self.insert_into_crawlqueue, interval=2),
            args=(ctx, ),
        )

        # db inserter thread - for new links
        ctx = Context(db=db)
        self.db_inserter_thread = Thread(
            target=run_forever(self.insert_into_db, interval=2),
            args=(ctx, ),
        )

        # db update thread - for any updates to existing links
        ctx = Context(db=db)
        self.db_update_thread = Thread(
            target=run_forever(self.update_db, interval=2),
            args=(ctx, ),
        )

        # db stats thread (has it's own connection)
        ctx = Context(db=PostgresDB(constants.DB_NAME))
        self.db_stats_thread = Thread(
            target=run_forever(self.send_db_stats, interval=10),
            args=(ctx, ),
        )

        # store thread
        ctx = Context()
        self.store_thread = Thread(
            target=run_forever(self.insert_into_store, interval=2),
            args=(ctx, ),
        )
        
        # batcher thread
        ctx = Context()
        self.batcher_thread = Thread(
            target=run_forever(self.batcher, interval=2),
            args=(ctx, ),
        )


        threads = [
            self.crawl_queue_inserter_thread,
            self.db_inserter_thread,
            self.db_update_thread,
            self.db_stats_thread,
            self.store_thread,
            self.batcher_thread,
        ]

        for t in threads:
            t.daemon = True
            t.start()

        # TODO add batching

    def insert_into_crawlqueue(self, ctx):

        buffer_almost_empty = self.crawl_queue.qsize() <= constants.CRAWL_QUEUE_LOW_WATERMARK
        if not buffer_almost_empty:
            logger.info("buffer not empty, not adding to crawlq")
            return

        num_links = self.crawl_queue.maxsize - self.crawl_queue.qsize()
        links = ctx.db.get_links_to_be_crawled(num_links=num_links)

        if len(links) == 0:
            return

        # if called too frequently, we will be getting links from db but never using them
        num_inserted = 0
        try:
            for link in links:
                self.crawl_queue.put_nowait(link)
                # NOTE could block if qsize is just "approximate"
                num_inserted += 1
        except:
            # should never reach this if crawl_queue.qsize is accurate
            logger.warn("CrawlQueue was full, qsize was inaccurate...")

        logger.info("inserted %d links into crawlq", num_inserted)
        stats.client.incr("crawlq.put", num_inserted)
        stats.client.gauge("urls.waiting.in_mem", self.crawl_queue.qsize())

        now = datetime.datetime.now()
        time_elapsed = now - ctx.last_successful_run
        ctx.last_successful_run = now

        stats.client.timing("insert_into_crawlqueue", time_elapsed.total_seconds() * 1000)

    def insert_into_db(self, ctx):
        while True:
            batch = []
            try:
                while True:
                    url = self.insert_into_db_queue.get(timeout=3)
                    batch.append(url)
                    if len(batch) >= constants.DB_QUEUE_BATCH_SIZE:
                        break
            except Empty:
                pass

            if len(batch) == 0:
                continue

            now = datetime.datetime.now()
            ctx.db.insert_batch(batch)
            logger.info("inserted %d links into db", len(batch))
            delta = datetime.datetime.now() - now
            stats.client.timing("db.insert_batch", delta.total_seconds() * 1000)

    def update_db(self, ctx):
        while True:
            batch = []
            try:
                while True:
                    item = self.update_db_queue.get(timeout=3)
                    batch.append(item)
                    if len(batch) >= constants.DB_QUEUE_BATCH_SIZE:
                        break
            except Empty:
                pass

            if len(batch) == 0:
                continue

            now = datetime.datetime.now()
            ctx.db.update_batch(batch)
            logger.info("updated %d links in db", len(batch))
            delta = datetime.datetime.now() - now
            stats.client.timing("db.update_batch", delta.total_seconds() * 1000)

    def batcher(self, ctx):
        while True:
            batch = []
            try:
                while True:
                    url = self.crawl_queue.get(timeout=3)
                    batch.append(url)
                    if len(batch) >= constants.WORKER_BATCH_SIZE:
                        break
            except Empty:
                pass

            if len(batch) == 0:
                continue

            self.batch_queue.put(batch)


    def insert_into_store(self, ctx):
        while True:
            # TODO dummy, not actually storing yet
            item = self.store_queue.get()
            pass

    def send_db_stats(self, ctx):
        counts = ctx.db.get_status_count()
        for status, count in counts.iteritems():
            stats.client.gauge("urls.%s" % status, count)

    def start_source(self, source):
        urls = self.hm.source_to_startup_links.get(source)
        assert urls is not None, "%r is not a valid source" % source
        for url in urls:
            self.insert_into_db_queue.put(url)

        return "enqueued %d startup urls" % len(urls)

    def get_http_endpoints(self):
        """
        returns a map of path to function
        """
        endpoints = {
            'start_source': self.start_source,
        }

        return endpoints

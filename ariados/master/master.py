import datetime
import logging
import time
from collections import defaultdict
from threading import Thread
from Queue import Queue, Empty

from ariados.common import constants
from ariados.common import stats
from ariados.handlermanager import HandlerManager
from ariados.master.store import JsonStore
from ariados.master.cockroach import Cockroach
from ariados.master.worker import Worker
from ariados.master.invokers import AWSLambdaInvoker
from ariados.master.db import Status
from ariados.utils import run_forever

logger = logging.getLogger(__name__)


class Master(object):
    def __init__(self, multiple=True):
        self.multiple = multiple
        self.threads = []
        self.invoker_factory = AWSLambdaInvoker
        self.hm = HandlerManager()

        self.init_crawl_queue()
        self.init_store()
        self.init_workers()
        self.init_service_threads()

        stats.client.gauge("urls.waiting.in_mem", 0)
        self.publish_constants_as_gauges()

    def publish_constants_as_gauges(self):
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

    def init_crawl_queue(self):
        self.crawl_queue = Queue(maxsize=constants.CRAWL_QUEUE_MAX_SIZE)
        self.new_links_queue = Queue(maxsize=constants.NEW_LINKS_QUEUE_MAX_SIZE)
        self.completed_queue = Queue(maxsize=constants.COMPLETED_QUEUE_MAX_SIZE)
        self.worker_queue = Queue(maxsize=constants.BATCH_QUEUE_MAX_SIZE)
        self.results_queue = Queue(maxsize=constants.RESULTS_QUEUE_MAX_SIZE)

    def init_store(self):
        self.store = JsonStore()

    def init_workers(self):
        worker_queue = self.worker_queue if self.multiple else self.crawl_queue
        self.workers = []
        for i in xrange(constants.NUM_WORKERS):
            w = Worker(self.invoker_factory, worker_queue,
                       self.new_links_queue, self.completed_queue,
                       self.results_queue, self.store)

            self.workers.append(w)
            self.threads.append(w)

            w.start()

    def init_service_threads(self):
        # links to db
        last_run = datetime.datetime.now() - datetime.timedelta(days=1)
        last_successful_run = last_run

        self.add_links_to_db_last_run = last_run
        self.add_links_to_db_last_successful_run = last_successful_run
        self.add_links_to_db_thread = Thread(
            target=run_forever(self.add_links_to_db, interval=2),
            args=(Cockroach(constants.DB_NAME), ),
        )
        self.add_links_to_db_thread.daemon = True
        self.add_links_to_db_thread.start()

        self.add_links_from_db_to_cq_last_run = last_run
        self.add_links_from_db_to_cq_last_successful_run = last_successful_run
        self.add_links_from_db_to_cq_thread = Thread(
            target=run_forever(self.add_links_from_db_to_cq, interval=2),
            args=(Cockroach(constants.DB_NAME), ),
        )
        self.add_links_from_db_to_cq_thread.daemon = True
        self.add_links_from_db_to_cq_thread.start()

        self.mark_crawling_complete_last_run = last_run
        self.mark_crawling_complete_last_successful_run = last_successful_run
        self.mark_crawling_complete_thread = Thread(
            target=run_forever(self.mark_crawling_complete, interval=2),
            args=(Cockroach(constants.DB_NAME), ),
        )
        self.mark_crawling_complete_thread.daemon = True
        self.mark_crawling_complete_thread.start()

        self.db_stats_thread = Thread(
            target=run_forever(self.send_db_stats, interval=10),
            args=(Cockroach(constants.DB_NAME), ),
        )
        self.db_stats_thread.daemon = True
        self.db_stats_thread.start()

        self.results_handler_thread = Thread(
            target=run_forever(self.results_handler, interval=0.1),
        )
        self.results_handler_thread.daemon = True
        self.results_handler_thread.start()

        self.threads.extend([
            self.add_links_to_db_thread,
            self.add_links_from_db_to_cq_thread,
            self.mark_crawling_complete_thread,
            self.results_handler_thread,
            self.db_stats_thread,
        ])

        if not self.multiple:
            return

        self.batcher_thread = Thread(
            target=run_forever(self.batcher, interval=0.1),
            args=(self.crawl_queue, self.worker_queue,),
        )
        self.batcher_thread.daemon = True
        self.batcher_thread.start()
        self.threads.append(self.batcher_thread)

    def add_links_to_db(self, db):
        now = datetime.datetime.now()
        delta = (now - self.add_links_to_db_last_run).total_seconds()
        hard_deadline_reached = (now - self.add_links_to_db_last_successful_run).total_seconds() >= constants.NEW_LINKS_QUEUE_MAX_WAIT_SECONDS

        load = (self.new_links_queue.qsize() / float(self.new_links_queue.maxsize)) * 100.0
        buffer_almost_full = load >= constants.NEW_LINKS_QUEUE_LOAD_FACTOR

        if not (hard_deadline_reached or buffer_almost_full):
            self.add_links_to_db_last_run = now
            return

        urls = set()
        try:
            while True:
                url = self.new_links_queue.get_nowait()
                urls.add(url)
        except Empty:
            pass

        urls = list(urls)
        start = 0
        end = constants.MAX_ENTRIES_PER_TRANSACTION
        chunk = urls[start:end]
        while len(chunk) > 0:
            db.insert_links(chunk)
            start = end
            end += constants.MAX_ENTRIES_PER_TRANSACTION
            chunk = urls[start:end]

        now = datetime.datetime.now()
        self.add_links_to_db_last_run = now
        self.add_links_to_db_last_successful_run = now
        logger.debug("added %d links to db", len(urls))

    def add_links_from_db_to_cq(self, db):
        now = datetime.datetime.now()
        delta = (now - self.add_links_from_db_to_cq_last_run).total_seconds()

        load = (self.crawl_queue.qsize() / float(self.crawl_queue.maxsize)) * 100.0
        buffer_almost_empty = load <= constants.CRAWL_QUEUE_LOAD_FACTOR

        hard_deadline_reached = (now - self.add_links_from_db_to_cq_last_successful_run).total_seconds() >= constants.CRAWL_QUEUE_MAX_SECONDS
        can_insert_more = (delta > constants.CRAWL_QUEUE_MIN_SECONDS) and buffer_almost_empty

        if not (hard_deadline_reached or can_insert_more):
            self.add_links_from_db_to_cq_last_run = now
            return

        num_links = self.crawl_queue.maxsize - self.crawl_queue.qsize()
        links = [ld["link"] for ld in db.get_links(num_links=num_links)]
        start = 0
        end = constants.MAX_ENTRIES_PER_TRANSACTION
        chunk = links[start:end]
        while len(chunk) > 0:
            db.update_links(chunk, Status.PROCESSING)
            for link in chunk:
                self.crawl_queue.put(link)
                stats.client.incr("crawlq.put")

            start = end
            end += constants.MAX_ENTRIES_PER_TRANSACTION
            chunk = links[start:end]

        stats.client.gauge("urls.waiting.in_mem", self.crawl_queue.qsize())
        now = datetime.datetime.now()
        self.add_links_from_db_to_cq_last_run = now
        self.add_links_from_db_to_cq_last_successful_run = now
        logger.debug("added %d links to cq", len(links))

    def mark_crawling_complete(self, db):
        now = datetime.datetime.now()
        delta = (now - self.mark_crawling_complete_last_run).total_seconds()
        load = (self.completed_queue.qsize() / float(self.completed_queue.maxsize)) / 100.0
        buffer_almost_full = load >= constants.COMPLETED_QUEUE_LOAD_FACTOR

        hard_deadline_reached = (now - self.mark_crawling_complete_last_successful_run).total_seconds() >= constants.COMPLETED_QUEUE_MAX_WAIT_SECONDS

        if not (hard_deadline_reached or buffer_almost_full):
            self.mark_crawling_complete_last_run = now
            return

        links_dict = defaultdict(list)
        try:
            while True:
                url, status = self.completed_queue.get_nowait()
                links_dict[status].append(url)
        except Empty:
            pass

        for status, links in links_dict.iteritems():
            start = 0
            end = constants.MAX_ENTRIES_PER_TRANSACTION
            chunk = links[start:end]
            while len(chunk) > 0:
                db.update_links(chunk, status=status)
                start = end
                end += constants.MAX_ENTRIES_PER_TRANSACTION
                chunk = links[start:end]

            logger.debug("marked %d links %s", len(links), status)

        now = datetime.datetime.now()
        self.mark_crawling_complete_last_run = now
        self.mark_crawling_complete_last_successful_run = now

    def send_db_stats(self, db):
        counts = db.get_status_count()
        for status, count in counts.iteritems():
            stats.client.gauge("urls.%s" % status, count)

    def batcher(self, crawl_queue, worker_queue):
        start_time = time.time()
        urls = []
        while True:
            try:
                url = crawl_queue.get(timeout=2)
                urls.append(url)
                if len(urls) >= constants.URL_BATCH_SIZE:
                    break
            except Empty:
                end_time = time.time()
                if (end_time - start_time) >= constants.URL_BATCH_MAX_WAIT_TIME:
                    break

        if len(urls) == 0:
            return

        worker_queue.put(urls)

    def handle_data(self, result):
        url = result['url']
        if not result['success']:
            self.completed_queue.put((url, Status.FAILED))
            logger.error("Failed to handle url %s", url)
            logger.error(result["error"])
            return

        self.completed_queue.put((url, Status.COMPLETED))
        data, links = result['data'], result['links']
        if data:
            self.store.store(data)

        for link in links:
            logger.debug("added new link %s", link)
            self.new_links_queue.put(link)
            # should we add this here or when we are adding the link to db?
            stats.client.incr("urls.new")

    def results_handler(self):
        while True:
            lambda_result = self.results_queue.get()
            if self.multiple:
                for lr in lambda_result:
                    self.handle_data(lr)
            else:
                self.handle_data(lambda_result)

    def crawl_url(self, url):
        """
        crawls a given url and returns the result
        """
        resp = self.invoker.handle_single_url(url)
        return resp

    def enqueue_url(self, url):
        self.new_links_queue.put_nowait(url)

    def start_source(self, source):
        urls = self.hm.source_to_startup_links.get(source)
        assert urls is not None, "%r is not a valid source" % source
        for url in urls:
            self.new_links_queue.put(url)

        return "enqueued %d startup urls" % len(urls)

    def get_http_endpoints(self):
        """
        returns a map of path to function
        """
        endpoints = {
            'crawl_url': self.crawl_url,
            'enqueue_url': self.enqueue_url,
            'start_source': self.start_source,
        }

        return endpoints

    def close(self):
        pass

import datetime
from collections import defaultdict
from threading import Thread
from Queue import Queue

from ariados.common import constants
from ariados.common import stats
from ariados.handlermanager import HandlerManager
from ariados.master.store import JsonStore
from ariados.master.cockroach import Cockroach
from ariados.master.worker import Worker
from ariados.master.invokers import AWSLambdaInvoker
from ariados.master.db import Status
from ariados.utils import run_forever

class Master(object):
    def __init__(self):
        self.threads = []
        self.invoker = AWSLambdaInvoker()
        self.hm = HandlerManager()

        self.init_crawl_queue()
        self.init_store()
        self.init_workers()
        self.init_service_threads()

        stats.client.gauge("lambdas.active", 0)
        stats.client.gauge("urls.waiting_on_disk", 0)
        stats.client.gauge("urls.waiting_in_mem", 0)
        stats.client.gauge("urls.in_progress.in_lambda", 0)
        stats.client.gauge("urls.completed", 0)
        stats.client.gauge("urls.failed", 0)

    def init_crawl_queue(self):
        self.crawl_queue = Queue(maxsize=constants.CRAWL_QUEUE_MAX_SIZE)
        self.new_links_queue = Queue(maxsize=constants.NEW_LINKS_QUEUE_MAX_SIZE)
        self.completed_queue = Queue(maxsize=constants.COMPLETED_QUEUE_MAX_SIZE)

    def init_store(self):
        self.store = JsonStore()

    def init_workers(self):
        self.workers = []
        for i in xrange(constants.NUM_WORKERS):
            w = Worker(self.invoker, self.crawl_queue,
                self.new_links_queue, self.completed_queue, self.store)

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

        self.threads.extend([
            self.add_links_to_db_thread,
            self.add_links_from_db_to_cq_thread,
            self.mark_crawling_complete_thread,
        ])

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
            break

        urls = list(urls)
        start = 0
        end = constants.MAX_ENTRIES_PER_TRANSACTION
        chunk = urls[start:end]
        while len(chunk) > 0:
            db.insert_links(chunk)
            stats.client.gauge("urls.waiting_on_disk", len(chunk), delta=True)
            start = end
            end += constants.MAX_ENTRIES_PER_TRANSACTION
            chunk = urls[start:end]

        now = datetime.datetime.now()
        self.add_links_to_db_last_run = now
        self.add_links_to_db_last_successful_run = now
        print "added %d links to db" % len(urls)

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
        db.update_links(links, Status.PROCESSING)

        stats.client.gauge("urls.waiting.in_mem", len(links), delta=True)
        stats.client.gauge("urls.waiting.on_disk", -len(links), delta=True)
        for link in links:
            self.crawl_queue.put(link)
            stats.client.incr("crawlq.put")

        now = datetime.datetime.now()
        self.add_links_from_db_to_cq_last_run = now
        self.add_links_from_db_to_cq_last_successful_run = now
        print "added %d links to cq" % len(links)

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
                stats.client.gauge("urls.%s" % status, len(chunk), delta=True)
                db.update_links(chunk, status=status)
                start = end
                end += constants.MAX_ENTRIES_PER_TRANSACTION
                chunk = links[start:end]

            print "marked %d links %s" % (len(links), status)

        now = datetime.datetime.now()
        self.mark_crawling_complete_last_run = now
        self.mark_crawling_complete_last_successful_run = now

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

import time
import logging

from threading import Thread
from Queue import Empty

from ariados.common import constants
from ariados.common import stats
from ariados.master.db import Status


logger = logging.getLogger(__name__)


class Worker(Thread):
    def __init__(self, invoker_factory, worker_queue, new_links_queue,
                 completed_queue, results_queue, store, multiple=True):
        super(Worker, self).__init__()
        self.daemon = True

        self.multiple = multiple
        self.worker_queue = worker_queue
        self.new_links_queue = new_links_queue
        self.completed_queue = completed_queue
        self.results_queue = results_queue
        self.store = store
        self.invoker = invoker_factory()

    def run_single(self):
        while True:
            try:
                url = self.worker_queue.get(timeout=2)
                stats.client.incr("crawlq.get")
            except Empty:
                continue

            logger.debug("Processing url: %s", url)
            try:
                result = self.invoker.handle_single_url(url)
                self.results_queue.put(result)
            except Exception:
                self.completed_queue.put((url, Status.FAILED))
                logger.error("Failed to handle url %s", url, exc_info=True)

    def run_multiple(self):
        while True:
            urls = self.worker_queue.get()
            stats.client.incr("crawlq.get", len(urls))
            logger.debug("Processing %d urls", len(urls))
            try:
                result = self.invoker.handle_multiple_urls(urls)
                self.results_queue.put(result)
            except Exception:
                for url in urls:
                    self.completed_queue.put((url, Status.FAILED))
                logger.error("Failed to handle %d urls", len(urls), exc_info=True)

    def run(self):
        if self.multiple:
            self.run_multiple()
        else:
            self.run_single()

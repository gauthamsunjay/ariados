import logging

from threading import Thread
from Queue import Empty

from ariados.common import stats
from ariados.master.db import Status


logger = logging.getLogger(__name__)


class Worker(Thread):
    def __init__(self, invoker_factory, crawl_queue, new_links_queue, completed_queue, store):
        super(Worker, self).__init__()
        self.daemon = True

        self.crawl_queue = crawl_queue
        self.new_links_queue = new_links_queue
        self.completed_queue = completed_queue
        self.store = store
        self.invoker = invoker_factory()

    def handle_data(self, result):
        data, links = result['data'], result['links']
        self.store.store(data)
        for link in links:
            logger.debug("added new link %s", link)
            self.new_links_queue.put(link)
            # should we add this here or when we are adding the link to db?
            stats.client.incr("urls.new")

    def run(self):
        while True:
            try:
                url = self.crawl_queue.get(timeout=5)
                stats.client.incr("crawlq.get")
            except Empty:
                continue

            logger.debug("Processing url: %s", url)
            try:
                result = self.invoker.handle_single_url(url)
            except Exception:
                self.completed_queue.put((url, Status.FAILED))
                logger.error("Failed to handle url %s", url, exc_info=True)
                continue

            if not result['success']:
                self.completed_queue.put((url, Status.FAILED))
                logger.error("Failed to handle url %s", url)
                logger.error(result["error"])
                continue

            self.completed_queue.put((url, Status.COMPLETED))
            self.handle_data(result)

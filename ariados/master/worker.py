import logging

from threading import Thread
from Queue import Empty

from ariados.common import stats, constants
from .pgdb import Status

logger = logging.getLogger(__name__)

class Worker(Thread):
    def __init__(self, invoker_factory, worker_queue, insert_queue, update_queue, store_queue,
            result_queue):
        super(Worker, self).__init__()

        self.invoker = invoker_factory()
        self.worker_queue = worker_queue
        self.insert_queue = insert_queue
        self.update_queue = update_queue
        self.store_queue = store_queue
        self.result_queue = result_queue

    def run_single(self):
        while True:
            url = self.worker_queue.get()
            stats.client.incr("crawlq.get")
            logger.debug("processing url: %s", url)
            status = Status.COMPLETED
            try:
                result = self.invoker.handle_single_url(url)

                if not result['success']:
                    status = Status.FAILED
                    logger.error("Failed to hande url %s", url)
                    logger.error(result["error"])
                else:
                    self.store_queue.put(result['data'])
                    for link in result['links']:
                        self.insert_queue.put(link)

            except Exception:
                status = Status.FAILED
                logger.error("failed to handle url %s", url, exc_info=True)

            finally:
                self.update_queue.put((url, status))

    def run_multiple(self):
        while True:
            batch = self.worker_queue.get()
            # stats.client.incr("crawlq.get", len(batch))

            try:
                results = self.invoker.handle_multiple_urls(batch)
                self.result_queue.put(results)
            except Exception:
                for url in batch:
                    self.update_queue.put((url, Status.FAILED))

                logger.error("Failed to handle %d urls", len(batch), exc_info=True)

    def run(self):
        self.run_multiple()

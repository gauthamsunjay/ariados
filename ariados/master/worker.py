import logging

from threading import Thread
from Queue import Empty

from ariados.common import stats, constants
from .pgdb import Status

logger = logging.getLogger(__name__)

class Worker(Thread):
    def __init__(self, invoker, worker_queue, update_queue):
        super(Worker, self).__init__()

        self.invoker = invoker
        self.worker_queue = worker_queue
        self.update_queue = update_queue

    def run_multiple(self):
        while True:
            batch = self.worker_queue.get()
            # stats.client.incr("crawlq.get", len(batch))

            try:
                self.invoker.handle_multiple_urls(batch)
            except Exception:
                logger.error("Failed to handle %d urls", len(batch), exc_info=True)
                for url in batch:
                    self.update_queue.put((url, Status.FAILED))


    def run(self):
        self.run_multiple()

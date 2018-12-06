import traceback

from threading import Thread
from Queue import Empty

from ariados.common import stats
from ariados.master.db import Status


class Worker(Thread):
    def __init__(self, invoker, crawl_queue, new_links_queue, completed_queue, store):
        super(Worker, self).__init__()
        self.daemon = True

        self.crawl_queue = crawl_queue
        self.new_links_queue = new_links_queue
        self.completed_queue = completed_queue
        self.store = store
        self.invoker = invoker

    def handle_data(self, result):
        data, links = result['data'], result['links']
        self.store.store(data)
        for link in links:
            print "added new link %s" % link
            self.new_links_queue.put(link)
            # should we add this here or when we are adding the link to db?
            stats.client.incr("urls.new")

    def run(self):
        while True:
            try:
                url = self.crawl_queue.get(timeout=5)
                stats.client.incr("crawlq.get")
                stats.client.gauge("urls.waiting.in_mem", -1, delta=True)
            except Empty:
                continue

            print "Processing url: %s" % url
            try:
                result = self.invoker.handle_single_url(url)
            except Exception:
                self.completed_queue.put((url, Status.FAILED))
                print "Failed to handle url %s" % url
                print traceback.format_exc()
                continue

            if not result['success']:
                self.completed_queue.put((url, Status.FAILED))
                print "Failed to handle url %s" % url
                print result["error"]
                continue

            self.completed_queue.put((url, Status.COMPLETED))
            self.handle_data(result)

import json
import traceback
from threading import Thread
from Queue import Empty


class Worker(Thread):
    def __init__(self, invoker, crawl_queue, new_links_queue, completed_queue, store):
        super(Worker, self).__init__()
        self.daemon = True

        self.crawl_queue = crawl_queue
        self.new_links_queue = new_links_queue
        self.completed_queue = completed_queue
        self.store = store
        self.invoker = invoker

    def run(self):
        while True:
            try:
                url = self.crawl_queue.get(timeout=5)
            except Empty:
                continue

            print "Processing url: %s" % url

            # TODO make handle_single_url return a good result always or throw exceptions.
            # these exceptions can be tried here to write a good response back to crawlqueue.
            result = self.invoker.handle_single_url(url)
            if not result['success']:
                # TODO log this failure
                print "Failed to crawl url %r, error: %s" % (url, result['error'])
                continue

            """
             Expected format for data:
             {
                "title": "", "body": {}, "links": [], ...
             }
            """
            data, links = result['data'], result['links']
            try:
                if data is not None and isinstance(data, dict):
                    data["url"] = url
                    self.store.store(data)

                if isinstance(links, list):
                    for link in links:
                        print "added new link %s" % link
                        self.new_links_queue.put(link)
                self.completed_queue.put(url)
            except:
                print "Got error for url %s" % url
                print traceback.format_exc()
                # TODO notify crawlqueue on failure so it doesn't retry to often.
                # something like a backoff

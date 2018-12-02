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

            resp = self.invoker.handle_single_url(url)
            # TODO make the invoker return a good response
            payload = json.loads(resp["Payload"].read().decode("utf-8"))
            if not isinstance(payload, list) and len(payload) != 2:
                print "Cannot handle payload %r" % payload
                continue

            """
             Expected format for data:
             {
                "title": "", "body": {}, "links": [], ...
             }
            """
            data, links = payload
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

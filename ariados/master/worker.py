import json
import traceback
from threading import Thread
from Queue import Empty

from ariados.common import stats

class Worker(Thread):
    def __init__(self, invoker, crawl_queue, new_links_queue, completed_queue, store):
        super(Worker, self).__init__()
        self.daemon = True

        self.crawl_queue = crawl_queue
        self.new_links_queue = new_links_queue
        self.completed_queue = completed_queue
        self.store = store
        self.invoker = invoker

        self.lambda_success_timer = stats.client.timer("lambdas.invocation.success")
        self.lambda_failure_timer = stats.client.timer("lambdas.invocation.failure")

    def start_lambda_timer(self):
        self.lambda_success_timer.start()
        self.lambda_failure_timer.start()

    def stop_lambda_timer(self, success=True):
        if success:
            self.lambda_success_timer.stop()
            self.lambda_failure_timer.stop(send=False)
        else:
            self.lambda_failure_timer.stop()
            self.lambda_success_timer.stop(send=False)

    def run(self):
        while True:
            try:
                url = self.crawl_queue.get(timeout=5)
                stats.client.incr("crawlq.get")
                stats.client.gauge("urls.waiting.in_mem", -1, delta=True)
            except Empty:
                continue

            print "Processing url: %s" % url

            # TODO make handle_single_url return a good result always or throw exceptions.
            # these exceptions can be tried here to write a good response back to crawlqueue.
            stats.client.incr("lambdas.invoked")
            stats.client.incr("lambdas.urls.in_progress")
            stats.client.gauge("lambdas.active", 1, delta=True)
            stats.client.gauge("urls.in_progress.in_lambda", 1, delta=True)
            self.start_lambda_timer()

            result = self.invoker.handle_single_url(url)
            stats.client.gauge("lambdas.active", -1, delta=True)
            stats.client.gauge("urls.in_progress.in_lambda", -1, delta=True)
            if not result['success']:
                # TODO log this failure
                self.stop_lambda_timer(success=False)
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
                        # should we add this here or when we are adding the link to db?
                        stats.client.incr("urls.new")

                self.completed_queue.put(url)

                self.stop_lambda_timer()
            except:
                self.stop_lambda_timer(success=False)
                print "Got error for url %s" % url
                print traceback.format_exc()
                # TODO notify crawlqueue on failure so it doesn't retry to often.
                # something like a backoff

import argparse
import json
import time
from datetime import datetime
from threading import Thread
from Queue import Queue, Empty

from aws import Function
from db import Status
from cockroach import Cockroach
from store import JsonStore

# config
DB_NAME = "crawl"
NUM_WORKERS = 5

CRAWL_QUEUE_MAX_SIZE = 3 * NUM_WORKERS
CRAWL_QUEUE_LOAD_FACTOR = 0.2
CRAWL_QUEUE_MIN_SECONDS = 5
CRAWL_QUEUE_MAX_SECONDS = 20

NEW_LINKS_QUEUE_MAX_SIZE = 3 * NUM_WORKERS
NEW_LINKS_QUEUE_LOAD_FACTOR = 0.7
NEW_LINKS_QUEUE_MAX_WAIT_SECONDS = 10

COMPLETED_QUEUE_MAX_SIZE = 3 * NUM_WORKERS
COMPLETED_QUEUE_LOAD_FACTOR = 0.7
COMPLETED_QUEUE_MAX_WAIT_SECONDS = 10


should_die = False


class Worker(Thread):
    def __init__(self, crawl_queue, fn, fn_name, new_links_queue,
                 completed_queue, store):
        super(Worker, self).__init__()
        self.crawl_queue = crawl_queue
        self.fn = fn
        self.fn_name = fn_name
        self.new_links_queue = new_links_queue
        self.completed_queue = completed_queue
        self.store = store

    def run(self):
        while True:
            try:
                url = self.crawl_queue.get(timeout=5)
            except Empty:
                if should_die:
                    break
                continue

            print "Processing url: %s" % url

            resp = self.fn.invoke(
                function_name=self.fn_name,
                invocation_type="RequestResponse",
                payload=json.dumps({"url": url})
            )
            data = json.loads(resp["Payload"].read().decode("utf-8"))
            """
             Expected format for data:
             {
                "title": "", "body": {}, "links": [], ...   
             }
            """
            try:
                if data["body"]:
                    self.store.store(data["title"], data["body"])
                self.completed_queue.put(url)
            except KeyError:
                print "url = %s, data = %s" % (url, data)

            for link in data.get("links", []):
                self.new_links_queue.put(link)


def get_args():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("url")
    return argparser.parse_args()


def add_links_to_db(db, new_links_queue):
    prev_time, cur_time = datetime.utcnow(), None

    while True:
        if should_die:
            break

        cur_time = datetime.utcnow()
        load = (new_links_queue.qsize() / float(new_links_queue.maxsize)) * 100.0
        delta = (cur_time - prev_time).total_seconds()

        if delta >= NEW_LINKS_QUEUE_MAX_WAIT_SECONDS or \
            load >= NEW_LINKS_QUEUE_LOAD_FACTOR:
            urls = [new_links_queue.get() for _ in range(new_links_queue.qsize())]
            db.insert_links(urls)
        else:
            time.sleep(2)


def add_links_from_db_to_cq(db, crawl_queue):
    prev_time, cur_time = datetime.utcnow(), None

    while True:
        if should_die:
            break

        cur_time = datetime.utcnow()
        load = (crawl_queue.qsize() / float(crawl_queue.maxsize)) * 100.0
        num_links = crawl_queue.maxsize - crawl_queue.qsize()
        delta = (cur_time - prev_time).total_seconds()

        if (delta > CRAWL_QUEUE_MIN_SECONDS and
                    load <= CRAWL_QUEUE_LOAD_FACTOR) or \
                (delta > CRAWL_QUEUE_MAX_SECONDS):

            prev_time = cur_time  # last time the queue was updated
            links = [ld["link"] for ld in db.get_links(num_links=num_links)]
            db.update_links(links, Status.PROCESSING)
            for link in links:
                crawl_queue.put(link)
        else:
            time.sleep(2)


def mark_crawling_complete(db, completed_queue):
    prev_time, cur_time = datetime.utcnow(), None

    while True:
        if should_die:
            break

        cur_time = datetime.utcnow()
        delta = (cur_time - prev_time).total_seconds()
        load = (completed_queue.qsize() / float(completed_queue.maxsize)) / 100.0

        if delta >= COMPLETED_QUEUE_MAX_WAIT_SECONDS or \
            load >= COMPLETED_QUEUE_LOAD_FACTOR:
            links = [completed_queue.get() for _ in range(completed_queue.qsize())]
            db.update_links(links, status=Status.COMPLETED)
        else:
            time.sleep(2)


def stop_threads_when_done(db):
    global should_die

    while True:
        waiting_link = db.get_link(status=Status.WAITING)
        processing_link = db.get_link(status=Status.PROCESSING)
        if waiting_link is None and processing_link is None:
            should_die = True
            break
        time.sleep(5)


def main(args):
    fn = Function() 
    fn_name = fn.register(
        function_name="uwcs_parser",
        handler="newparser.run_parser",
        description="parser for uw computer science pages",
        zip_file="./uwcs_parser.zip"
    )

    crawl_queue = Queue(maxsize=CRAWL_QUEUE_MAX_SIZE)
    new_links_queue = Queue(maxsize=NEW_LINKS_QUEUE_MAX_SIZE)
    completed_queue = Queue(maxsize=COMPLETED_QUEUE_MAX_SIZE)

    crawl_queue.put(args.url)
    store = JsonStore()

    worker_threads = [
        Worker(crawl_queue, fn, fn_name, new_links_queue,
               completed_queue, store) for _ in range(NUM_WORKERS)
    ]

    service_threads = [
        Thread(target=add_links_to_db, args=(Cockroach(DB_NAME),
                                             new_links_queue)),
        Thread(target=add_links_from_db_to_cq, args=(Cockroach(DB_NAME),
                                                     crawl_queue)),
        Thread(target=mark_crawling_complete, args=(Cockroach(DB_NAME),
                                                    completed_queue))
    ]

    threads = worker_threads + service_threads

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    args = get_args()
    main(args)

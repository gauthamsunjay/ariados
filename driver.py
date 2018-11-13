import argparse
import json
import re
import urlparse
from threading import Thread
from Queue import Queue, Empty

from aws import Function

NUM_WORKERS = 5

allowed_schemes = frozenset(('https', 'http'))
domains_to_regex_fn_map = {
    'www.cs.wisc.edu': {
        re.compile(r'/events/\d+/?'): True,
        re.compile(r'/calendar/month/\d{4}-\d{2}/?'): True,
    },
}

def is_valid_url(url):
    parsed_url = urlparse.urlparse(url)
    if parsed_url.scheme not in allowed_schemes:
        return False

    if parsed_url.netloc not in domains_to_regex_fn_map:
        return False

    match = False
    for regex in domains_to_regex_fn_map[parsed_url.netloc].iterkeys():
        match = regex.match(parsed_url.path)
        if match is not None:
            return True

    return False

class Worker(Thread):
    def __init__(self, queue, fn, fn_name, visited_urls):
        super(Worker, self).__init__()
        self.queue = queue
        self.fn = fn
        self.fn_name = fn_name
        self.visited_urls = visited_urls

    def run(self):
        while True:
            url = self.queue.get()
            print "Processing url: %s" % url

            resp = self.fn.invoke(
                function_name=self.fn_name,
                invocation_type="RequestResponse",
                payload=json.dumps({"url": url})
            )
            data = json.loads(resp["Payload"].read().decode("utf-8"))
            try:
                body = json.loads(data["body"])
                if body["data"]:
                    print body["data"]
            except KeyError:
                print "Url = %s, data = %s" % (url, data)

            for link in body["links"]:
                if not is_valid_url(link):
                    continue

                if link not in self.visited_urls:
                    self.visited_urls.add(link)
                    self.queue.put(link)

            if len(self.visited_urls) >= 50:
                break


def get_args():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("url")
    return argparser.parse_args()


def main(args):
    fn = Function() 
    fn_name = fn.register(
        function_name="uwcs_parser",
        handler="newparser.run_parser",
        description="parser for uw computer science pages",
        zip_file="./uwcs_parser.zip"
    )

    queue = Queue()
    queue.put(args.url)
    visited_urls = set()

    threads = [Worker(queue, fn, "uwcs_parser", visited_urls) for _ in range(NUM_WORKERS)]
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    args = get_args()
    main(args)


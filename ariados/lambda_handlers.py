import itertools
import traceback

from collections import namedtuple
from Queue import Queue
from threading import Thread

import requests

from ariados.handlermanager import HandlerManager

# TODO add logging - level based on optional event config

class Poison(object):
    pass

class Empty(object):
    pass

ErrorItem = namedtuple("ErrorItem", ["idx", "traceback"])
FetcherInput = namedtuple("FetcherInput", ["idx", "url"])
ProcessorInput = namedtuple("ProcessorInput", ["idx", "resp"])
ProcessorOutput = namedtuple("ProcessorOutput", ["idx", "data", "links"])

def fetch(inp):
    resp = requests.get(inp.url, timeout=3, allow_redirects=False) # TODO hav a configurable timoeut?
    return ProcessorInput(idx=inp.idx, resp=resp)

def fetcher(fetch_q, process_q):
    """
    reads from the fetch queue and puts the response into the process queue.
    this function will always put one entry into the process queue for an entry from fetch queue
    regardless of whether it succeeds or not.
    dies on Poison
    """

    while True:
        inp = fetch_q.get()
        if inp is Poison:
            break

        # NOTE: fetch_q can only get FetcherInput or Poison!
        assert isinstance(inp, FetcherInput), "expected FetcherInput but got %r" % type(inp)

        try:
            processor_inp = fetch(inp)
            process_q.put(processor_inp)
        except:
            process_q.put(ErrorItem(idx=inp.idx, traceback=traceback.format_exc()))

def process(hm, inp):
    assert isinstance(inp, ProcessorInput), "expected ProcessorInput but got %r" % type(inp)

    inp.resp.raise_for_status()
    if inp.resp.is_redirect:
        c_link = hm.canonicalize_url(inp.resp.headers["Location"])
        if hm.get_handler_for_url(c_link) is not None:
            return ProcessorOutput(idx=inp.idx, data={}, links=[c_link])


    # TODO should we canonicalize? Do we assume canonical urls are provided to us?
    # is it cheap to do canonicalization here?
    handler = hm.get_handler_for_url(inp.resp.url)
    if handler is None:
        raise Exception("No handler for url %r" % inp.resp.url)

    data, links = handler(inp.resp)
    canon_links = set()
    for link in links:
        c_link = hm.canonicalize_url(link)
        if hm.get_handler_for_url(c_link) is not None:
            canon_links.add(c_link)

    return ProcessorOutput(idx=inp.idx, data=data, links=list(canon_links))

def processor(hm, process_q, output_q):
    """
    reads from the process queue and puts the processed output into the output queue.
    this function will always put one entry into the output queue for an entry from the process queue regardless of whether it succeeds or not.
    dies on Poison.
    """

    assert isinstance(hm, HandlerManager)

    while True:
        inp = process_q.get()
        if inp is Poison:
            break

        if isinstance(inp, ErrorItem):
            output_q.put(inp)
            continue

        try:
            output = process(hm, inp)
            output_q.put(output)
        except:
            output_q.put(ErrorItem(idx=inp.idx, traceback=traceback.format_exc()))

def handle_single_url(event, context):
    url = event.get('url', None)
    if url is None:
        return {'success': False, 'error': 'expected "url" in payload'}

    hm = HandlerManager()
    fetch_q = Queue(maxsize=2)
    process_q = Queue(maxsize=2)
    output_q = Queue(maxsize=1)

    fetch_q.put(FetcherInput(idx=0, url=url))
    fetch_q.put(Poison)
    fetcher(fetch_q, process_q)

    process_q.put(Poison)
    processor(hm, process_q, output_q)

    output = output_q.get()
    if isinstance(output, ErrorItem):
        return { 'url': url, 'success': False, 'error': output.traceback }

    return {'url': url, 'success': True, 'data': output.data, 'links': output.links }

def handle_multiple_urls(event, context):
    urls = event.get('urls', None)
    if urls is None:
        return {'success': False, 'error': 'expected "urls" in payload'}

    if not isinstance(urls, list):
        return {'success': False, 'error': 'expected "urls" to be a list'}

    # TODO have some threshold for multiple urls. Otherwise just process them sequentially?
    hm = HandlerManager()
    # TODO what is the right maxsize?
    fetch_q = Queue(maxsize=len(urls))
    process_q = Queue(maxsize=len(urls))
    output_q = Queue(maxsize=len(urls))

    # TODO make these configurable
    num_fetchers = 4
    num_processors = 2
    fetchers = [Thread(target=fetcher, args=[fetch_q, process_q]) for i in range(num_fetchers)]
    processors = [Thread(target=processor, args=[hm, process_q, output_q]) for i in range(num_processors)]

    for i in itertools.chain(fetchers, processors):
        i.daemon = True
        i.start()

    for idx, url in enumerate(urls):
        fetch_q.put(FetcherInput(idx, url))

    for f in fetchers:
        fetch_q.put(Poison)

    for f in fetchers:
        f.join()

    for p in processors:
        process_q.put(Poison)

    for p in processors:
        p.join()

    output = []
    for i in xrange(len(urls)):
        out = output_q.get()
        if isinstance(out, ErrorItem):
            output.append({
                "success": False,
                "idx": out.idx,
                "url": urls[out.idx],
                "error": out.traceback,
            })

            continue

        # NOTE has to be either ProcessorOutput or ErrorItem. nothing else.
        assert isinstance(out, ProcessorOutput)

        output.append({
            "success": True,
            "idx": out.idx,
            "url": urls[out.idx],
            "data": out.data,
            "links": out.links,
        })

    output.sort(key=lambda x: x['idx'])
    return { 'success': True, 'result': output }

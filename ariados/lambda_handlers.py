import itertools
from collections import namedtuple
from Queue import Queue
from threading import Thread

import requests

from ariados.handlermanager import HandlerManager

# TODO add logging - level based on optional event config

def handle_single_url(event, context):
    url = event.get('url', None)
    if url is None:
        return

    hm = HandlerManager()
    # assumes the url is canonicalized.
    handler = hm.get_handler_for_url(url)
    if handler is None:
        return

    resp = requests.get(url)
    assert 200 <= resp.status_code <= 299
    data, links = handler(resp)

    canon_links = []
    for link in links:
        canon_link = hm.canonicalize_url(link)
        if hm.get_handler_for_url(canon_link) is not None:
            canon_links.append(canon_link)

    return data, canon_links

class Poison(object):
    pass

class Empty(object):
    pass

FetcherInp = namedtuple("FetcherInp", ["idx", "url"])
ProcessorInp = namedtuple("ProcessorInp", ["fetcher_inp", "resp"])
ProcessorOut = namedtuple("ProcessorOut", ["processor_inp", "data", "links"])

def _fetcher(in_q, out_q):
    while True:
        inp = in_q.get()
        if inp is Poison:
            break

        if not isinstance(inp, FetcherInp):
            # TODO log this
            out_q.put(Empty)
            continue

        # TODO this can fail as well
        resp = requests.get(inp.url)
        out_q.put(ProcessorInp(inp, resp))

def _processor(hm, in_q, out_q):
    while True:
        inp = in_q.get()
        if inp is Poison:
            break

        if not isinstance(inp, ProcessorInp):
            # TODO log this
            out_q.put(Empty)
            continue

        # TODO Propagate a more meaningful error
        if not 200 <= inp.resp.status_code <= 299:
            out_q.put(Empty)
            continue

        handler = hm.get_handler_for_url(inp.resp.url)
        if handler is None:
            out_q.put(Empty)
            continue

        output = Empty
        try:
            data, links = handler(inp.resp)
            canon_links = []
            for link in links:
                c_link = hm.canonicalize_url(link)
                if hm.get_handler_for_url(c_link) is not None:
                    canon_links.append(c_link)

            output = ProcessorOut(inp, data, links)
        except:
            # Propagate a more meaningful error
            pass

        out_q.put(output)

def handle_multiple_urls(event, context):
    urls = event.get('urls', None)
    if urls is None:
        return

    if not isinstance(urls, list):
        return

    # TODO have some threshold for multiple urls. Otherwise just process them sequentially?
    hm = HandlerManager()
    # TODO what is the right maxsize?
    fetcher_inp_q = Queue(maxsize=len(urls))
    processor_inp_q = Queue(maxsize=len(urls))
    out_q = Queue(maxsize=len(urls))

    # TODO make these configurable
    num_fetchers = 4
    num_processors = 2
    fetchers = [Thread(target=_fetcher, args=[fetcher_inp_q, processor_inp_q]) for i in range(num_fetchers)]
    processors = [Thread(target=_processor, args=[hm, processor_inp_q, out_q]) for i in range(num_processors)]

    for i in itertools.chain(fetchers, processors):
        i.start()

    for idx, url in enumerate(urls):
        fetcher_inp_q.put(FetcherInp(idx, url))

    for f in fetchers:
        fetcher_inp_q.put(Poison)

    for f in fetchers:
        f.join()

    for p in processors:
        processor_inp_q.put(Poison)

    for p in processors:
        p.join()

    output = []
    for i in xrange(len(urls)):
        out = out_q.get()
        # TODO what if it caused an error? Send that back to client
        if not isinstance(out, ProcessorOut):
            continue

        output.append({
            "idx": out.processor_inp.fetcher_inp.idx,
            "url": out.processor_inp.fetcher_inp.url,
            "data": out.data,
            "links": out.links,
        })

    output.sort(key=lambda x: x['idx'])
    return output

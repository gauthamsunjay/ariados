import requests

from ariados.handlermanager import HandlerManager

def handler(event, context):
    # TODO handle crawling multiple urls
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



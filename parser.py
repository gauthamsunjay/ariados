import argparse
import json
import re
import sys
import urlparse
from collections import OrderedDict
from StringIO import StringIO

import requests
from lxml import etree

def parse_event_page(resp):
    parser = etree.HTMLParser()
    data = resp.content
    root = etree.parse(StringIO(data), parser)

    title = root.xpath('//h1[@id="page-title"]/text()')[0]
    date = root.xpath('//div[contains(@class, "field-name-field-date")]//div[contains(@class, "field-item")]//span/@content')
    start_time = date[0]
    end_time = None
    if len(date) == 2:
        end_time = date[1]

    loc = root.xpath('//div[contains(@class, "field-name-field-location")]//text()')[0]
    speaker_name = root.xpath('//section[contains(@class, "field-name-field-speaker-name")]//div/text()')[0]
    speaker_ins = root.xpath('//section[contains(@class, "field-name-field-speaker-institution")]//div/text()')[0]
    body = root.xpath('//section[contains(@class, "field-name-body")]//p/text()')

    result = OrderedDict()
    result['title'] = title.strip()
    result['start_time'] = start_time
    result['end_time'] = end_time
    result['location'] = loc
    result['speaker'] = { 'name': speaker_name, 'from': speaker_ins }
    result['body'] = body

    return result, []

def parse_calendar_page(resp):
    parser = etree.HTMLParser()
    data = resp.content
    root = etree.parse(StringIO(data), parser)

    # TODO give every link or only event links?
    # Should we let the caller decide and filter?

    # Optimization: filtering here since it's my module
    links = []
    for link in root.xpath('//a/@href'):
        full_link = urlparse.urljoin(resp.url, link)
        links.append(full_link)
    return None, links

allowed_schemes = frozenset(('https', 'http'))
domains_to_regex_fn_map = {
    frozenset(('cs.wisc.edu', 'www.cs.wisc.edu')): {
        r'/events/\d+/?': parse_event_page,
        r'/calendar/month/\d{4}-\d{2}/?': parse_calendar_page,
    },
}

# TODO make sure this cannot be changed.
domain_to_regex_fn_map = {}
for domains, regex_fn_map in domains_to_regex_fn_map.iteritems():
    new_regex_fn_map = {}
    for regex, fn in regex_fn_map.iteritems():
        new_regex_fn_map[re.compile(regex)] = fn

    for domain in domains:
        domain_to_regex_fn_map[domain] = new_regex_fn_map

def process_url(url):
    parsed_url = urlparse.urlparse(url)
    assert parsed_url.scheme in allowed_schemes, "invalid scheme %s" % parsed_url.scheme
    assert parsed_url.netloc in domain_to_regex_fn_map, "invalid domain %s" % parsed_url.netloc

    regex_fn_map = domain_to_regex_fn_map[parsed_url.netloc]
    regex, fn = None, None
    for _regex, _fn in regex_fn_map.iteritems():
        if _regex.match(parsed_url.path):
            regex = _regex
            fn = _fn
            break

    assert all((regex, fn)), "could not find regex/fn for path %s" % parsed_url.path

    resp = requests.get(url)
    assert 200 <= resp.status_code <= 299
    return fn(resp)

def run_parser(event, context):
    url = event["url"]
    data, links = process_url(url)
    return {
        "statusCode": 200,
        "body": json.dumps({"data": data, "links": links})
    }

def get_args():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("url")
    return argparser.parse_args()

def main(args):
    data, links = process_url(args.url)
    print json.dumps(data, indent=2)
    print json.dumps(links, indent=2)

if __name__ == '__main__':
    args = get_args()
    main(args)

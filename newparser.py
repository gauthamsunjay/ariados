import argparse
import json
import re
import sys
import urlparse
from collections import OrderedDict
from StringIO import StringIO

import requests
from bs4 import BeautifulSoup

def parse_event_page(resp):
    data = resp.content
    soup = BeautifulSoup(data, 'html.parser')
    title = soup.select_one('h1#page-title').string
    #dates = soup.select('div.field-name-field-date  div.field-item > span.date-display-single')
    #start_time = dates[0]['content']
    #end_time = None
    #if len(date) == 2:
    #    end_time = dates[1]['content']

    loc = soup.select_one('div.field-name-field-location').string
    speaker_name = soup.select_one('section.field-name-field-speaker-name div.field-items').string
    speaker_ins = soup.select_one('section.field-name-field-speaker-institution div.field-items').string
    body = soup.select_one('section.field-name-body').text

    result = OrderedDict()
    result['title'] = title.strip()
    #result['start_time'] = start_time
    #result['end_time'] = end_time
    result['location'] = loc
    result['speaker'] = { 'name': speaker_name, 'from': speaker_ins }
    result['body'] = body

    return result, []

def parse_calendar_page(resp):
    data = resp.content
    soup = BeautifulSoup(data, 'html.parser')

    # TODO give every link or only event links?
    # Should we let the caller decide and filter?
    # Optimization: filtering here since it's my module?

    links = soup.find_all('a')
    links = filter(None, map(lambda x: x.get('href'), links))
    links = map(lambda x: urlparse.urljoin(resp.url, x), links)
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
        'statusCode': 200,
        'body': json.dumps({"data": data, "links": links})
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

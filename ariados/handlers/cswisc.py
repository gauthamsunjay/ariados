import urlparse
from collections import OrderedDict

from bs4 import BeautifulSoup

from ariados.decorators import handler

SOURCE = 'cswisc'
DOMAINS = ['cs.wisc.edu', 'www.cs.wisc.edu']
STARTUP_LINKS = [
    "https://www.cs.wisc.edu/calendar/month/{year}-{month}".format(year=year, month=month)
    for year in (2018, 2017, 2016, 2015)
    for month in range(1, 13),
]

# TODO figure out how to reuse the regex in @handler. Maybe use the named groups from re.match?
def canonicalize_url(url):
    url = urlparse.urlparse(url)
    pr = urlparse.ParseResult(
        scheme=url.scheme, netloc=url.netloc, path=url.path.rstrip('/'),
        params='', query='', fragment='')
    return urlparse.urlunparse(pr)

# TODO make handler take some allowed status codes. By default only 200-299
@handler(r'^/events/\d+$')
def parse_events(resp):
    data = resp.content
    soup = BeautifulSoup(data, 'html.parser')
    title = soup.select_one('h1#page-title').string
    start_time = soup.select('div.field-name-field-date  div.field-item  span.date-display-start')
    if len(start_time) > 0:
        start_time = start_time[0]['content']
    else:
        start_time = None

    end_time = soup.select('div.field-name-field-date  div.field-item  span.date-display-end')
    if len(end_time) > 0:
        end_time = end_time[0]['content']
    else:
        end_time = None

    loc = soup.select_one('div.field-name-field-location').string
    speaker_name = soup.select_one('section.field-name-field-speaker-name div.field-items').string
    speaker_ins = soup.select_one('section.field-name-field-speaker-institution div.field-items').string
    body = soup.select_one('section.field-name-body').text

    result = OrderedDict()
    result['title'] = title.strip()
    result['start_time'] = start_time
    result['end_time'] = end_time
    result['location'] = loc
    result['speaker'] = { 'name': speaker_name, 'from': speaker_ins }
    result['body'] = body

    return result, []

@handler(r'^/calendar/month/\d{4}-\d{2}$')
def parse_calendar_page(resp):
    data = resp.content
    soup = BeautifulSoup(data, 'html.parser')

    # TODO giving every link. It is upto the caller to filter these links using
    # handlermanager

    links = soup.find_all('a')
    links = filter(None, map(lambda x: x.get('href'), links))
    links = map(lambda x: urlparse.urljoin(resp.url, x), links)
    return None, links

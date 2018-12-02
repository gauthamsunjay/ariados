import urlparse

from collections import OrderedDict
from lxml import etree
from StringIO import StringIO

from ariados.decorators import handler

SOURCE = 'cmu'
DOMAINS = ['www.cmu.edu', 'www.cs.cmu.edu']
STARTUP_LINKS = ['https://www.cs.cmu.edu/calendar']


def canonicalize_url(url):
    url = urlparse.urlparse(url)
    pr = urlparse.ParseResult(
        scheme=url.scheme, netloc=url.netloc, path=url.path.rstrip('/'),
        params='', query=url.query, fragment='')
    return urlparse.urlunparse(pr)


@handler(r"^/calendar/(mon|tue|wed|thu|fri|sat|sun)-\d{4}-\d{2}-\d{2}-\d{4}/.*")
def parse_events(resp):
    data = resp.content
    parser = etree.HTMLParser()
    tree = etree.parse(StringIO(data), parser)

    title = tree.xpath("//h1[@id='page-title']/text()")
    title = title[0].strip() if title else ""

    event_type = tree.xpath("//div[contains(@class,'event__label')]/text()")
    event_type = event_type[0].strip() if event_type else ""

    day = tree.xpath("//div[contains(@class, 'event__date')]/time[@datetime]/text()")
    day = day[0].strip() if day else ""
    if day.endswith("-"):
        day = day.rstrip("-").strip()

    start_time = tree.xpath("//div[contains(@class, 'event__date')]//span[contains(@class,'date-display-start')]/text()")
    start_time = start_time[0].strip() if start_time else ""

    end_time = tree.xpath("//div[contains(@class, 'event__date')]//span[contains(@class,'date-display-end')]/text()")
    end_time = end_time[0].strip() if end_time else ""

    location = tree.xpath("//ul[contains(@class, 'event__location-information semantic')]/li/text()")
    location = "\n".join(location).strip()

    body = tree.xpath("//div[contains(@class, 'event__content')]/descendant::node()/text()")
    body = "\n".join(body).strip()

    speaker = tree.xpath("//li[contains(@class, 'event__speaker-name')]/descendant::node()/text()")
    speaker = speaker[0].strip() if speaker else ""

    result = OrderedDict()
    result["title"] = title
    result["event_type"] = event_type
    result["start_time"] = start_time
    result["end_time"] = end_time
    result["day"] = day
    result["location"] = location
    result["speaker"] = speaker
    result["body"] = body

    return result, []


@handler(r'^/calendar($|\?page=.*)')
def parse_calendar(resp):
    data = resp.content
    parser = etree.HTMLParser()
    tree = etree.parse(StringIO(data), parser)

    links = tree.xpath("//ul[@class='events-teasers-list semantic']//a/@href")
    links = map(lambda link: urlparse.urljoin(resp.url, link), links)

    return None, links

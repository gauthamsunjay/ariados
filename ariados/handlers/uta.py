import urlparse

from collections import OrderedDict
from lxml import etree
from StringIO import StringIO

from ariados.decorators import handler

SOURCE = "uta"

no_www_domains = set(['www.jsg.utexas.edu', 'www.ices.utexas.edu', 'www.mccombs.utexas.edu', 'www.engr.utexas.edu', 'www.ischool.utexas.edu', u'www.utexas.edu'])

www_domains = set(['sites.utexas.edu', 'maps.utexas.edu', 'cmhc.utexas.edu', 'diversity.utexas.edu', 'gradschool.utexas.edu', 'econnect.utexas.edu', 'soa.utexas.edu', 'moody.utexas.edu', 'news.utexas.edu', 'socialwork.utexas.edu', 'nursing.utexas.edu', 'experts.utexas.edu', 'austin.uteach.utexas.edu', 'extendedcampus.utexas.edu', 'dellmedschool.utexas.edu', 'titleix.utexas.edu', 'law.utexas.edu', 'education.utexas.edu', 'emergency.utexas.edu', 'cns.utexas.edu', 'admissions.utexas.edu', 'finearts.utexas.edu', 'utdirect.utexas.edu'])

DOMAINS = list(www_domains.union(no_www_domains))
startup_link_set = set()
startup_link_set.add("https://www.utexas.edu/academics/areas-of-study")
for i in DOMAINS:
    startup_link_set.add("https://%s" % i)

STARTUP_LINKS = list(startup_link_set)

# adding the www versions and non www versions to each so it can be detected here
DOMAINS.extend([ 'www.%s' % i for i in no_www_domains ])
DOMAINS.extend([ i[4:] for i in www_domains ])
DOMAINS = list(set(DOMAINS))


def canonicalize_url(url):
    url = urlparse.urlparse(url)
    domain = url.netloc
    if domain.startswith("www."):
        if domain not in www_domains:
            domain = domain[4:]
    else:
        if domain not in no_www_domains:
            domain = "www.%s" % domain

    if domain not in www_domains and domain not in no_www_domains:
        return "http://lets-not-crawl-this.com"

    path = url.path.rstrip('/')
    splits = path.rsplit('.', 1)
    if len(splits) == 2 and splits[-1] != "html":
        return "http://lets-not-crawl-this.com"

    pr = urlparse.ParseResult(
        scheme=url.scheme, netloc=domain, path=path,
        params='', query=url.query, fragment='')

    return urlparse.urlunparse(pr)

@handler(r'.*')
def parse_everything(resp):
    data = resp.content
    parser = etree.HTMLParser()
    tree = etree.parse(StringIO(data), parser)

    try:
        title = tree.xpath('//head/title/text()')[0]
    except:
        title = "Title Not Found"

    text = ''
    body_tag = tree.xpath('body')
    if len(body_tag) > 0:
        body_tag = body_tag[0]
        text = ''.join(body_tag.itertext())

    data = {
        'url': resp.url,
        'title': title,
        'text': text,
    }

    links = tree.xpath('//a/@href')
    links = map(lambda x: urlparse.urljoin(resp.url, x), links)
    return data, links

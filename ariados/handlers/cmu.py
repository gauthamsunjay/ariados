import urlparse

from collections import OrderedDict
from lxml import etree
from StringIO import StringIO

from ariados.decorators import handler

SOURCE = 'cmu'

DOMAINS = [
    'www.analytics.tepper.cmu.edu', 'www.ms-product-management.cmu.edu', 'www.music.cmu.edu', 'www.neon.mems.cmu.edu', 'www.sv.cmu.edu',
    'www.arc.cmu.edu', 'www.australia.cmu.edu', 'www.bme.cmu.edu', 'www.cbd.cmu.edu', 'www.cfa.cmu.edu', 'www.chem.cmu.edu',
    'www.cheme.cmu.edu', 'www.cmu.edu', 'www.csd.cs.cmu.edu', 'www.design.cmu.edu', 'www.drama.cmu.edu', 'www.ece.cmu.edu',
    'www.epp.cmu.edu', 'www.etc.cmu.edu', 'www.hcii.cmu.edu', 'www.heinz.cmu.edu', 'www.hss.cmu.edu', 'www.ini.cmu.edu',
    'www.isri.cmu.edu', 'www.isri.cs.cmu.edu', 'www.lti.cs.cmu.edu', 'www.math.cmu.edu', 'www.ml.cmu.edu', 'www.psy.cmu.edu',
    'www.ri.cmu.edu', 'www.stat.cmu.edu', 'www.tepper.cmu.edu',
]

startup_link_set = set()
startup_link_set.add("https://www.cmu.edu/sitemap/index.html")
for i in DOMAINS:
    startup_link_set.add(i)

STARTUP_LINKS = list(startup_link_set)

def canonicalize_url(url):
    url = urlparse.urlparse(url)
    pr = urlparse.ParseResult(
        scheme=url.scheme, netloc=url.netloc, path=url.path.rstrip('/'),
        params='', query=url.query, fragment='')
    return urlparse.urlunparse(pr)

@handler(r'.*')
def parse_everything(resp):
    data = resp.content
    parser = etree.HTMLParser()
    tree = etree.parse(StringIO(data), parser)

    title = tree.xpath('//head/title/text()')[0]
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

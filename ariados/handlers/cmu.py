import urlparse

from collections import OrderedDict
from lxml import etree
from StringIO import StringIO

from ariados.decorators import handler

SOURCE = 'cmu'

www_domains = [
    'www.analytics.tepper.cmu.edu', 'www.ms-product-management.cmu.edu', 'www.music.cmu.edu', 'www.neon.mems.cmu.edu', 'www.sv.cmu.edu',
    'www.arc.cmu.edu', 'www.australia.cmu.edu', 'www.bme.cmu.edu', 'www.cbd.cmu.edu', 'www.cfa.cmu.edu', 'www.chem.cmu.edu',
    'www.cheme.cmu.edu', 'www.cmu.edu', 'www.csd.cs.cmu.edu', 'www.design.cmu.edu', 'www.drama.cmu.edu', 'www.ece.cmu.edu',
    'www.epp.cmu.edu', 'www.etc.cmu.edu', 'www.hcii.cmu.edu', 'www.heinz.cmu.edu', 'www.hss.cmu.edu', 'www.ini.cmu.edu',
    'www.isri.cmu.edu', 'www.isri.cs.cmu.edu', 'www.lti.cs.cmu.edu', 'www.math.cmu.edu', 'www.ml.cmu.edu', 'www.psy.cmu.edu',
    'www.ri.cmu.edu', 'www.stat.cmu.edu', 'www.tepper.cmu.edu',
]

no_www_domains = [i[4:] for i in www_domains if i.startswith('www.')]

# although canon is www, we need no_www to ensure the detection happens
DOMAINS = list(set(www_domains + no_www_domains))

startup_link_set = set()
startup_link_set.add("https://www.cmu.edu/sitemap/index.html")
for i in www_domains:
    startup_link_set.add("https://%s" % i)

STARTUP_LINKS = list(startup_link_set)

def canonicalize_url(url):
    # always adds www to the url in addition to normal stuff
    url = urlparse.urlparse(url)
    domain = url.netloc
    if not domain.startswith("www."):
        domain = "www.%s" % domain

    # NOTE we do not want any big files or images or anything like that
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

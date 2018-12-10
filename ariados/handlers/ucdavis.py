import urlparse

from collections import OrderedDict
from lxml import etree
from StringIO import StringIO

from ariados.decorators import handler

SOURCE = 'ucdavis'

www_domains = set(['www.gsm.ucdavis.edu', 'www.alumni.ucdavis.edu', 'www.ucdmc.ucdavis.edu', 'www.vetmed.ucdavis.edu', 'www.ucdavis.edu', 'www.ls.ucdavis.edu', 'www.law.ucdavis.edu'])

no_www_domains = set(['mobile.ucdavis.edu', 'ucdavismagazine.ucdavis.edu', 'education.ucdavis.edu', 'biology.ucdavis.edu', 'cpe.ucdavis.edu', 'icc.ucdavis.edu', 'orientation.ucdavis.edu', 'tag.ucdavis.edu', 'engineering.ucdavis.edu', 'academicsenate.ucdavis.edu', 'my.ucdavis.edu', 'research.ucdavis.edu', 'sdps.ucdavis.edu', 'canvas.ucdavis.edu', 'occr.ucdavis.edu', 'gradstudies.ucdavis.edu', 'myadmissions.ucdavis.edu', 'staff.ucdavis.edu', 'campusmap.ucdavis.edu', 'giving.ucdavis.edu',
    'caes.ucdavis.edu', 'visit.ucdavis.edu', 'leadership.ucdavis.edu', 'recruit.ucdavis.edu', 'alumni.ucdavis.edu', 'sisweb.ucdavis.edu', 'registrar.ucdavis.edu', 'shcs.ucdavis.edu'])

DOMAINS = list(www_domains.union(no_www_domains))
startup_link_set = set()
startup_link_set.add("https://www.ucdavis.edu/academics/colleges-schools")
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

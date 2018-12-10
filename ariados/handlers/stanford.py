import urlparse

from collections import OrderedDict
from lxml import etree
from StringIO import StringIO

from ariados.decorators import handler

SOURCE = 'stanford'

no_www_domains = set(['stanfordcareers.stanford.edu', 'aa.stanford.edu', 'structuralbio.stanford.edu', 'alumni.stanford.edu', 'cmgm.stanford.edu', 'wasc.stanford.edu', 'engineering.stanford.edu', 'uit.stanford.edu', 'obgyn.stanford.edu', 'statistics.stanford.edu', 'chemsysbio.stanford.edu', 'religiousstudies.stanford.edu', 'art.stanford.edu', 'interdisciplinary.stanford.edu', 'msande.stanford.edu', 'library.stanford.edu', 'radiology.stanford.edu', 'campus-map.stanford.edu',
    'classics.stanford.edu', 'taps.stanford.edu', 'admission.stanford.edu', 'neurobiology.stanford.edu', 'linguistics.stanford.edu', 'studentaffairs.stanford.edu', 'mathematics.stanford.edu', 'mse.stanford.edu', 'giving.stanford.edu', 'surgery.stanford.edu', 'finaid.stanford.edu', 'neurosurgery.stanford.edu', 'adminguide.stanford.edu', 'appliedphysics.stanford.edu', 'hrp.stanford.edu', 'comm.stanford.edu', 'stanfordwho.stanford.edu', 'bioengineering.stanford.edu', 'humsci.stanford.edu',
    'exploredegrees.stanford.edu', 'pathology.stanford.edu', 'dermatology.stanford.edu', 'radonc.stanford.edu', 'politicalscience.stanford.edu', 'devbio.stanford.edu', 'psychology.stanford.edu', 'genetics.stanford.edu', 'law.stanford.edu', 'psychiatry.stanford.edu', 'ed.stanford.edu', 'urology.stanford.edu', 'ealc.stanford.edu', 'music.stanford.edu', 'chemistry.stanford.edu', 'visit.stanford.edu', 'ctsurgery.stanford.edu', 'physics.stanford.edu', 'parents.stanford.edu', 'english.stanford.edu',
    'cee.stanford.edu', 'earth.stanford.edu', 'cs.stanford.edu', 'profiles.stanford.edu', 'med.stanford.edu', 'facts.stanford.edu', 'philosophy.stanford.edu', 'emergency.stanford.edu', 'me.stanford.edu', 'gsb.stanford.edu', 'pediatrics.stanford.edu', 'cheme.stanford.edu', 'ee.stanford.edu', 'ortho.stanford.edu', 'medicine.stanford.edu', 'history.stanford.edu', 'biology.stanford.edu', 'online.stanford.edu', 'sociology.stanford.edu', 'dlcl.stanford.edu', 'mcp.stanford.edu', 'biochem.stanford.edu',
    'economics.stanford.edu', 'ophthalmology.stanford.edu', 'itunes.stanford.edu', 'anthropology.stanford.edu'])

www_domains = set(['www.stanford.edu', 'www.law.stanford.edu', 'www.gsb.stanford.edu'])

DOMAINS = list(www_domains.union(no_www_domains))
startup_link_set = set()
startup_link_set.add("https://www.stanford.edu/list/academic/")
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

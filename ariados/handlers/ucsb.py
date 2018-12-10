import urlparse

from collections import OrderedDict
from lxml import etree
from StringIO import StringIO

from ariados.decorators import handler

SOURCE = 'ucsb'

www_domains = set(['www.ucsb.edu', 'www.anth.ucsb.edu', 'www.math.ucsb.edu', 'www.esm.ucsb.edu', 'www.music.ucsb.edu', 'www.gss.ucsb.edu', 'www.summer.ucsb.edu', 'www.ece.ucsb.edu', 'www.chem.ucsb.edu', 'www.classics.ucsb.edu', 'www.complit.ucsb.edu', 'www.chicst.ucsb.edu', 'www.pstat.ucsb.edu', 'www.frit.ucsb.edu', 'www.soc.ucsb.edu', 'www.ems.ucsb.edu', 'www.blackstudies.ucsb.edu', 'www.lais.ucsb.edu', 'www.es.ucsb.edu', 'www.housing.ucsb.edu', 'www.jewishstudies.ucsb.edu', 'www.femst.ucsb.edu',
    'www.library.ucsb.edu', 'www.history.ucsb.edu', 'www.identity.ucsb.edu', 'www.milsci.ucsb.edu', 'www.speech.ucsb.edu', 'www.research.ucsb.edu', 'www.religion.ucsb.edu', 'www.arthistory.ucsb.edu', 'www.comm.ucsb.edu', 'www.physics.ucsb.edu', 'www.global.ucsb.edu', 'www.geog.ucsb.edu', 'www.theaterdance.ucsb.edu', 'www.english.ucsb.edu', 'www.polsci.ucsb.edu', 'www.mcdb.ucsb.edu', 'www.geol.ucsb.edu', 'www.asamst.ucsb.edu', 'www.msi.ucsb.edu', 'www.filmandmedia.ucsb.edu', 'www.bmse.ucsb.edu',
    'www.graddiv.ucsb.edu', 'www.psych.ucsb.edu', 'www.linguistics.ucsb.edu', 'www.news.ucsb.edu', 'www.cs.ucsb.edu', 'www.philosophy.ucsb.edu', 'www.writing.ucsb.edu', 'www.spanport.ucsb.edu', 'www.tmp.ucsb.edu', 'www.materials.ucsb.edu', 'www.eastasian.ucsb.edu', 'www.chemengr.ucsb.edu', 'www.mat.ucsb.edu', 'www.econ.ucsb.edu', 'www.alumni.ucsb.edu', 'www.arts.ucsb.edu'])

no_www_domains = set(['undergrad.research.ucsb.edu', 'me.ucsb.edu', 'lifesci.ucsb.edu', 'secure.identity.ucsb.edu', 'education.ucsb.edu', 'oeosh.ucsb.edu', 'giving.ucsb.edu', 'osl.sa.ucsb.edu', 'my.sa.ucsb.edu', 'recreation.sa.ucsb.edu', 'essr.sa.ucsb.edu', 'ccs.ucsb.edu', 'wellness.sa.ucsb.edu', 'renaissancestudies.ucsb.edu', 'medievalstudies.ucsb.edu', 'extension.ucsb.edu'])

DOMAINS = list(www_domains.union(no_www_domains))
startup_link_set = set()
startup_link_set.add("https://www.ucsb.edu/academics/academic-departments-and-programs")
for i in DOMAINS:
    startup_link_set.add("https://%s" % i)

STARTUP_LINKS = list(startup_link_set)

# addming the www versions and non www versions to each so it can be detected here
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

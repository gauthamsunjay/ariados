import urlparse

from collections import OrderedDict
from lxml import etree
from StringIO import StringIO

from ariados.decorators import handler

SOURCE = 'ucla'

www_domains = set(['www.ucla.edu', 'www.disabilitystudies.ucla.edu', 'www.cjs.ucla.edu', 'www.bioeng.ucla.edu', 'www.semel.ucla.edu', 'www.biolchem.ucla.edu', 'www.milsci.ucla.edu', 'www.aud.ucla.edu', 'www.religion.ucla.edu', 'www.soc.ucla.edu', 'www.art.ucla.edu', 'www.dma.ucla.edu', 'www.mcdb.ucla.edu', 'www.linguistics.ucla.edu', 'www.chemeng.ucla.edu', 'www.honors.ucla.edu', 'www.archaeology.ucla.edu', 'www.ee.ucla.edu', 'www.pies.ucla.edu', 'www.uei.ucla.edu', 'www.registrar.ucla.edu', 'www.geronet.ucla.edu', 'www.music.ucla.edu', 'www.asianam.ucla.edu', 'www.eeb.ucla.edu', 'www.ioes.ucla.edu', 'www.commstudies.ucla.edu', 'www.neurosci.ucla.edu', 'www.genderstudies.ucla.edu', 'www.appling.ucla.edu', 'www.psych.ucla.edu', 'www.neurology.ucla.edu', 'www.classics.ucla.edu', 'www.events.ucla.edu', 'www.nelc.ucla.edu', 'www.math.ucla.edu', 'www.tft.ucla.edu', 'www.italian.ucla.edu', 'www.mimg.ucla.edu', 'www.med.ucla.edu', 'www.ethnomusic.ucla.edu', 'www.polisci.ucla.edu', 'www.iranian.ucla.edu', 'www.truebruin.ucla.edu', 'www.biomath.ucla.edu', 'www.alc.ucla.edu', 'www.cs.ucla.edu', 'www.directory.ucla.edu', 'www.nursing.ucla.edu', 'www.spanport.ucla.edu', 'www.bioinformatics.ucla.edu', 'www.biomedresearchminor.ucla.edu', 'www.germanic.ucla.edu', 'www.genetics.ucla.edu', 'www.history.ucla.edu', 'www.musicology.ucla.edu', 'www.emergency.ucla.edu', 'www.russian.ucla.edu', 'www.ph.ucla.edu', 'www.mse.ucla.edu', 'www.wp.ucla.edu', 'www.complit.ucla.edu', 'www.mcip.ucla.edu', 'www.labor.ucla.edu', 'www.mae.ucla.edu', 'www.mbi.ucla.edu', 'www.neuroscience.ucla.edu', 'www.anthro.ucla.edu', 'www.biostat.ucla.edu', 'www.scandinavian.ucla.edu', 'www.dentistry.ucla.edu', 'www.french.ucla.edu', 'www.atmos.ucla.edu', 'www.wacd.ucla.edu', 'www.ess.ucla.edu', 'www.wac.ucla.edu', 'www.slavic.ucla.edu', 'www.physci.ucla.edu', 'www.astro.ucla.edu', 'www.philosophy.ucla.edu', 'www.neurobio.ucla.edu', 'www.econ.ucla.edu', 'www.english.ucla.edu', 'www.afro-am.ucla.edu', 'www.bmp.ucla.edu', 'www.law.ucla.edu', 'www.chavez.ucla.edu', 'www.americanindianstudies.ucla.edu', 'www.chemistry.ucla.edu', 'www.arthistory.ucla.edu', 'www.physiology.ucla.edu', 'www.anderson.ucla.edu', 'www.publicaffairs.ucla.edu', 'www.pharmacology.ucla.edu', 'www.afrotc.ucla.edu', 'www.navy.ucla.edu', 'www.geog.ucla.edu'])

no_www_domains = set(['volunteer.ucla.edu', 'home.physics.ucla.edu', 'luskin.ucla.edu', 'campusservices.ucla.edu', 'luskinconferencecenter.ucla.edu', 'mias.gseis.ucla.edu', 'is.gseis.ucla.edu', 'giveto.ucla.edu', 'equity.ucla.edu', 'main.transportation.ucla.edu', 'm.ucla.edu', 'cee.ucla.edu', 'pathology.ucla.edu', 'statistics.ucla.edu', 'gseis.ucla.edu', 'lgbtstudies.ucla.edu', 'conservation.ucla.edu', 'socgen.ucla.edu', 'web.international.ucla.edu', 'cdh.ucla.edu', 'hpm.ph.ucla.edu'])

DOMAINS = list(www_domains.union(no_www_domains))
startup_link_set = set()
startup_link_set.add("http://www.ucla.edu/academics/departments-and-programs")
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

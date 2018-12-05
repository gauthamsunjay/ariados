import urlparse

from collections import OrderedDict
from lxml import etree
from StringIO import StringIO

from ariados.decorators import handler

SOURCE = 'wisc'
DOMAINS = ['wisc.edu']

subdomains = [
    'aae', 'advising', 'afroamericanstudies', 'afrotc', 'agroecology', 'agronomy', 'alc', 'amindian', 'anatomy', 'anesthesia', 'ansci',
    'anthropology', 'art', 'arthistory', 'arts', 'asianamerican', 'astro', 'bact', 'badgerrotc', 'biochem', 'biology', 'biostat', 'bmolchem',
    'botany', 'bse', 'bus', 'cals', 'canes', 'canvas', 'celticstudies', 'chem', 'chicla', 'ci', 'clfs', 'commarts', 'compliance',
    'continuingstudies', 'crb', 'creeca', 'csd', 'dance', 'dces', 'dermatology', 'diversity',
    'dpla', 'eastasia', 'econ', 'education', 'emed', 'english', 'engr', 'entomology', 'envirosci', 'fammed', 'foodsci', 'forestandwildlifeecology',
    'frit', 'genetics', 'geography', 'geoscience', 'global', 'globalcultures', 'gns', 'grad', 'guide', 'history', 'histsci', 'horticulture', 'humonc',
    'info', 'integrativebiology', 'international', 'ischool', 'ismajor', 'it', 'jewishstudies', 'jobs', 'journalism', 'kinesiology', 'lacis', 'lafollette',
    'languages', 'law', 'library', 'ling', 'ls', 'lsc', 'madison', 'math', 'mcardle', 'med', 'medhist', 'medievalstudies', 'medmicro', 'medphysics',
    'meteor', 'mideast', 'mobile', 'molpharm', 'music', 'nelson', 'neuro', 'neurology', 'neurosurg', 'news', 'nutrisci', 'obe', 'obgyn', 'ophth', 'orthorehab',
    'parent', 'pathology', 'pediatrics', 'pharmacology', 'pharmacy', 'philosophy', 'physics', 'physiology', 'plantpath', 'polisci', 'polyglot', 'pophealth',
    'psych', 'psychiatry', 'pubs', 'radiology', 'registrar', 'religiousstudies', 'research', 'rpse', 'seasia', 'secfac', 'socwork', 'sohe', 'soils', 'son',
    'southasia', 'spanport', 'ssc', 'stat', 'sts', 'students', 'studyabroad', 'surgery', 'theatre', 'urology', 'uwpd', 'vetmed', 'womenstudies', 'working', 'wsb',
]

for i in subdomains:
    DOMAINS.append("%s.wisc.edu" % i)

www_domains = ["www.%s" % i for i in DOMAINS]
DOMAINS.extend(www_domains)

startup_link_set = set()
startup_link_set.add("https://www.wisc.edu")
for i in subdomains:
    startup_link_set.add("https://www.%s.wisc.edu" % i)


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

import urlparse

from collections import OrderedDict
from lxml import etree
from StringIO import StringIO

from ariados.decorators import handler

SOURCE = "uci"

no_www_domains = set(['www.research.uci.edu', 'www.ota.uci.edu', 'www.cfep.uci.edu', 'www.physsci.uci.edu', 'www.arts.uci.edu', 'www.humanities.uci.edu', 'www.ofas.uci.edu', 'www.senate.uci.edu', 'www.nursing.uci.edu', 'www.law.uci.edu', 'www.reg.uci.edu', 'www.meded.uci.edu', 'www.cie.uci.edu', 'www.som.uci.edu', 'www.bio.uci.edu', 'www.admissions.uci.edu', 'www.fm.uci.edu', 'www.accreditation.uci.edu', 'www.urop.uci.edu', 'www.eng.uci.edu', 'www.pharmsci.uci.edu', 'www.editor.uci.edu', 'www.socsci.uci.edu', 'www.grad.uci.edu', 'www.ics.uci.edu', 'www.due.uci.edu'])


www_domains = set(['give.uci.edu', 'students.uci.edu', 'law.uci.edu', 'communications.uci.edu', 'admissions.uci.edu', 'honors.uci.edu', 'education.uci.edu', 'disability.uci.edu', u'uci.edu', 'police.uci.edu', 'innovation.uci.edu', 'directory.uci.edu', 'provost.uci.edu', 'faculty.uci.edu', 'summer.uci.edu', 'lib.uci.edu', 'socialecology.uci.edu', 'housing.uci.edu', 'pharmsci.uci.edu', 'merage.uci.edu', 'today.uci.edu', 'ics.uci.edu', 'cohs.uci.edu', 'alumni.uci.edu', 'som.uci.edu', 'portal.uci.edu', 'strategicplan.uci.edu', 'chancellor.uci.edu', 'ce.uci.edu', 'websoc.reg.uci.edu', 'research.uci.edu', 'publichealth.uci.edu', 'engage.uci.edu', 'catalogue.uci.edu', 'webmail.uci.edu', 'urop.uci.edu', 'parents.uci.edu', 'calteach.uci.edu', 'conferencecenter.uci.edu', 'forms.communications.uci.edu', 'eee.uci.edu'])


DOMAINS = list(www_domains.union(no_www_domains))
startup_link_set = set()
startup_link_set.add("https://uci.edu/academics/index.php")
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

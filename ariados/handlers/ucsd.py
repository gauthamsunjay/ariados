import urlparse

from collections import OrderedDict
from lxml import etree
from StringIO import StringIO

from ariados.decorators import handler

SOURCE = "ucsd"

no_www_domains = set(['transportation.ucsd.edu', 'cte.ucsd.edu', 'microarrays.ucsd.edu', 'sandera.ucsd.edu', 'shipsked.ucsd.edu', 'healthbeat.ucsd.edu', 'mytravel.ucsd.edu', 'literature.ucsd.edu', 'alumni.ucsd.edu', 'developer.ucsd.edu', 'ff21.ucsd.edu', 'jobs.ucsd.edu', 'cbam.ucsd.edu', 'cfr.ucsd.edu', 'math.ucsd.edu', 'cilas.ucsd.edu', 'mediaservices.ucsd.edu', 'chinesestudies.ucsd.edu', 'bikeshop.ucsd.edu', 'basicwriting.ucsd.edu', 'campus-lisa.ucsd.edu', 'oruba.ucsd.edu', 'cancer.ucsd.edu',
    'jacobsschool.ucsd.edu', 'cmbb.ucsd.edu', 'provost.ucsd.edu', 'foundation.ucsd.edu', 'preuss.ucsd.edu', 'ucsdfoundation.ucsd.edu', 'cucea.ucsd.edu', 'softreserves.ucsd.edu', 'psychology.ucsd.edu', 'hshr.ucsd.edu', 'weber.ucsd.edu', 'invent.ucsd.edu', 'studentfoundation.ucsd.edu', 'palprogram.ucsd.edu', 'biomedsci.ucsd.edu', 'accreditation.ucsd.edu', 'supportgroups.ucsd.edu', 'hdh.ucsd.edu', 'financiallink.ucsd.edu', 'pstp.ucsd.edu', 'library.ucsd.edu', 'ihouse.ucsd.edu',
    'myapplication.ucsd.edu', 'raidivision.ucsd.edu', 'religion.ucsd.edu', 'roger.ucsd.edu', 'vcsa.ucsd.edu', 'paths.ucsd.edu', 'innovation.ucsd.edu', 'cit.ucsd.edu', 'molpath.ucsd.edu', 'inc2.ucsd.edu', 'som.ucsd.edu', 'genetics.ucsd.edu', 'changemaker.ucsd.edu', 'students.ucsd.edu', 'fao.ucsd.edu', 'physicalplanning.ucsd.edu', 'ccom.ucsd.edu', 'hcsi.ucsd.edu', 'mytritonlink.ucsd.edu', 'warren.ucsd.edu', 'ph.ucsd.edu', 'repromed.ucsd.edu', 'women.ucsd.edu', 'crafts.ucsd.edu',
    'globalhealthprogram.ucsd.edu', 'hem-onc.ucsd.edu', 'stuartcollection.ucsd.edu', 'emergencymed.ucsd.edu', 'nanoengineering.ucsd.edu', 'isp.ucsd.edu', 'calendar.ucsd.edu', 'epidemiology.ucsd.edu', u'ucsd.edu', 'parents.ucsd.edu', 'koreanstudies.ucsd.edu', 'ling.ucsd.edu', 'artsandhumanities.ucsd.edu', 'econweb.ucsd.edu', 'rmp.ucsd.edu', 'igcc.ucsd.edu', 'endocrinology.ucsd.edu', 'iew.ucsd.edu', 'srs.ucsd.edu', 'academicaffairs.ucsd.edu', 'management.ucsd.edu', 'socialsciences.ucsd.edu',
    'sls.ucsd.edu', 'classweb.ucsd.edu', 'acms.ucsd.edu', 'swe.ucsd.edu', 'recreation.ucsd.edu', 'stemcells.ucsd.edu', 'psychservices.ucsd.edu', 'fdc-pds.ucsd.edu', 'ogme.ucsd.edu', 'scripps.ucsd.edu', 'downsyndrome.ucsd.edu', 'giving.ucsd.edu', 'studentconduct.ucsd.edu', 'eyesite.ucsd.edu', 'ccb.ucsd.edu', 'cdip.ucsd.edu', 'ipe.ucsd.edu', 'gastro.ucsd.edu', 'mbtg.ucsd.edu', 'studentsustainability.ucsd.edu', 'pharmacology.ucsd.edu', 'familymedresidency.ucsd.edu', 'pdel.ucsd.edu',
    'career.ucsd.edu', 'courses.ucsd.edu', 'thecolleges.ucsd.edu', 'chancellor.ucsd.edu', 'cmm.ucsd.edu', 'crl.ucsd.edu', 'cgmh.ucsd.edu', 'geropsych.ucsd.edu', 'sdbreathofhope.ucsd.edu', 'iphone.ucsd.edu', 'rels.ucsd.edu', 'writingcenter.ucsd.edu', 'adminrecords.ucsd.edu', 'sociology.ucsd.edu', 'biochemgen.ucsd.edu', 'volunteer.ucsd.edu', 'admissions.ucsd.edu', 'ecegsc.ucsd.edu', 'ocga.ucsd.edu', 'hedricklab.ucsd.edu', 'extension.ucsd.edu', 'libraries.ucsd.edu', 'gcf.ucsd.edu', 'irb.ucsd.edu',
    'tpot.ucsd.edu', 'oasis.ucsd.edu', 'sioms.ucsd.edu', 'pao.ucsd.edu', 'hds.ucsd.edu', 'matsci.ucsd.edu', 'philosophy.ucsd.edu', 'urp.ucsd.edu', 'empathy.ucsd.edu', 'cmg.ucsd.edu', 'trio.ucsd.edu', 'glaucoma.ucsd.edu', 'sbs.ucsd.edu', 'brianschultzgolfclassic.ucsd.edu', 'vmg.ucsd.edu', 'scw.ucsd.edu', 'tritonlink.ucsd.edu', 'diversity.ucsd.edu', 'ucpath.ucsd.edu', 'sustainability.ucsd.edu', 'gsa.ucsd.edu', 'medgenetics.ucsd.edu', 'ncmir.ucsd.edu', 'gps.ucsd.edu', 'sciencebridge.ucsd.edu',
    'libguides.ucsd.edu', 'nanomedicine.ucsd.edu', 'kibm.ucsd.edu', 'facultyexcellence.ucsd.edu', 'facultycouncil.ucsd.edu', 'darkstar.ucsd.edu', '5k.ucsd.edu', 'genomes.ucsd.edu', 'capital.ucsd.edu', 'universityartgallery.ucsd.edu', 'hpwren.ucsd.edu', 'medpeds.ucsd.edu', 'ets.ucsd.edu', 'townandgown.ucsd.edu', 'healthycampus.ucsd.edu', 'postdoc.ucsd.edu', 'meded.ucsd.edu', 'emeriti.ucsd.edu', 'dermatology.ucsd.edu', 'revelle.ucsd.edu', 'eds.ucsd.edu', 'disabilities.ucsd.edu', 'eaop.ucsd.edu',
    'sharecase.ucsd.edu', 'newmarketplace.ucsd.edu', 'tritoned.ucsd.edu', 'kidscience.ucsd.edu', 'corebio.ucsd.edu', 'gorderwalk.ucsd.edu', 'maeweb.ucsd.edu', 'neurosciences.ucsd.edu', 'research.ucsd.edu', 'communication.ucsd.edu', 'surgery.ucsd.edu', 'sixth.ucsd.edu', 'cdps.ucsd.edu', 'catering.ucsd.edu', 'marshall.ucsd.edu', 'japan.ucsd.edu', 'visarts.ucsd.edu', 'fiesta.ucsd.edu', 'sdawp.ucsd.edu', 'statusofwomen.ucsd.edu', 'rrdp.ucsd.edu', 'healthsciences.ucsd.edu', 'friends.ucsd.edu',
    'history.ucsd.edu', 'cwo.ucsd.edu', 'freespeech.ucsd.edu', 'esys.ucsd.edu', 'usp.ucsd.edu', 'bookstore.ucsd.edu', 'sarc.ucsd.edu', 'fudan-uc.ucsd.edu', 'summersession.ucsd.edu', 'psychiatry.ucsd.edu', 'universitycenters.ucsd.edu', 'earthguide.ucsd.edu', 'fmph.ucsd.edu', 'create.ucsd.edu', 'facclub.ucsd.edu', 'physiology.ucsd.edu', 'brc.ucsd.edu', 'nrs.ucsd.edu', 'bhmscholarship.ucsd.edu', 'iwd.ucsd.edu', 'hospitalmedicine.ucsd.edu', 'employeelink.ucsd.edu', 'ctri.ucsd.edu', 'med.ucsd.edu',
    '100g.ucsd.edu', 'globalties.ucsd.edu', 'itrc.ucsd.edu', 'physicalsciences.ucsd.edu', 'biology.ucsd.edu', 'empac.ucsd.edu', 'nephrology.ucsd.edu', 'commons.ucsd.edu', 'cddi.ucsd.edu', 'crbs.ucsd.edu', 'grad.ucsd.edu', 'mathtesting.ucsd.edu', 'grtc.ucsd.edu', 'cihed.ucsd.edu', 'act.ucsd.edu', 'lchc.ucsd.edu', 'onthego.ucsd.edu', 'ir.ucsd.edu', 'cbc.ucsd.edu', 'apply.grad.ucsd.edu', 'cancertraining.ucsd.edu', 'athletics.ucsd.edu', 'cgs.ucsd.edu', 'igpp.ucsd.edu', 'plandesignbuild.ucsd.edu',
    'rady.ucsd.edu', 'studentresearch.ucsd.edu', 'cmrr.ucsd.edu', 'wic.ucsd.edu', 'sira.ucsd.edu', 'registrar.ucsd.edu', 'blink.ucsd.edu', 'imresidency.ucsd.edu', 'jimo.ucsd.edu', 'casswww.ucsd.edu', 'bmes.ucsd.edu', 'academicconnections.ucsd.edu', 'geriatricsmedicine.ucsd.edu', 'studenthealth.ucsd.edu', 'status.ucsd.edu', 'tesc.ucsd.edu', 'procurement.ucsd.edu', 'mbrd.ucsd.edu', 'moores.ucsd.edu', 'ucsdnews.ucsd.edu', 'muir.ucsd.edu', 'health.ucsd.edu', 'ombuds.ucsd.edu', 'sscf.ucsd.edu',
    'nbmu.ucsd.edu', 'cfar.ucsd.edu', 'pathology.ucsd.edu', 'pharmacy.ucsd.edu', 'immunology.ucsd.edu', 'its.ucsd.edu', 'ophd.ucsd.edu', 'facilities.ucsd.edu', 'ia.ucsd.edu', 'roosevelt.ucsd.edu', 'evcstaffhr.ucsd.edu', 'studyabroad.ucsd.edu', 'resnet.ucsd.edu', 'sciencestudies.ucsd.edu', 'ucpa.ucsd.edu', 'muscle.ucsd.edu', 'ccc.ucsd.edu', 'lawandsociety.ucsd.edu', 'id.ucsd.edu', 'datalink.ucsd.edu', 'digitallearning.ucsd.edu', 'online.ucsd.edu', 'structures.ucsd.edu', 'polisci.ucsd.edu',
    'cer.ucsd.edu', 'child.ucsd.edu', 'sportsfac.ucsd.edu', 'judaicstudies.ucsd.edu', 'residency.ucsd.edu', 'usmex.ucsd.edu', 'educationinitiative.ucsd.edu', 'iem.ucsd.edu', 'genmed.ucsd.edu', 'tigs.ucsd.edu', 'cardiology.ucsd.edu', 'glycotech.ucsd.edu', 'mobile.ucsd.edu', 'algae.ucsd.edu', 'idi.ucsd.edu', 'pulmonary.ucsd.edu', 'bssa.ucsd.edu', 'fdc.ucsd.edu', 'iicas.ucsd.edu', 'boxoffice.ucsd.edu', 'music.ucsd.edu', 'caesar.ucsd.edu', 'ieee.ucsd.edu', 'icenter.ucsd.edu', 'economics.ucsd.edu',
    'chd.ucsd.edu', 'mandeville.ucsd.edu', 'lgbt.ucsd.edu', 'bsp.ucsd.edu'])

DOMAINS = list(no_www_domains)
startup_link_set = set()
for i in DOMAINS:
    startup_link_set.add("https://%s" % i)

STARTUP_LINKS = list(startup_link_set)

# adding the www versions and non www versions to each so it can be detected here
DOMAINS.extend([ 'www.%s' % i for i in no_www_domains ])
DOMAINS = list(set(DOMAINS))

def canonicalize_url(url):
    url = urlparse.urlparse(url)
    domain = url.netloc
    if domain.startswith("www."):
        domain = domain[4:]

    if domain not in no_www_domains:
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

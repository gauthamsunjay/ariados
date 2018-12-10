import urlparse

from collections import OrderedDict
from lxml import etree
from StringIO import StringIO

from ariados.decorators import handler

SOURCE = "ucb"

no_www_domains = set(["caldining.berkeley.edu", "thecenter.berkeley.edu", "caclgbt.berkeley.edu", "blu.berkeley.edu", "myPower.berkeley.edu", "sciencereview.berkeley.edu", "cheme.berkeley.edu", "chemistry.berkeley.edu", "webnet.berkeley.edu", "calstudentstore.berkeley.edu", "riskservices.berkeley.edu", "globalengagement.berkeley.edu", "africam.berkeley.edu", "rems.berkeley.edu", "calcentral.berkeley.edu", "visit.berkeley.edu", "geography.berkeley.edu", "gse.berkeley.edu", "myyears.berkeley.edu",
    "mackcenter.berkeley.edu", "fpf.berkeley.edu", "publicsafety.berkeley.edu", "ethics.berkeley.edu", "ieas.berkeley.edu", "bulletin.berkeley.edu", "vpsafp.berkeley.edu", "healthsciences.berkeley.edu", "scholarships.berkeley.edu", "stronach.berkeley.edu", "cse.ssl.berkeley.edu", "data.berkeley.edu", "eps.berkeley.edu", "ga.berkeley.edu", "businessinnovation.berkeley.edu", "guide.berkeley.edu", "ugiscommencement.berkeley.edu", "survivorsupport.berkeley.edu", "newscenter.berkeley.edu",
    "leadership.berkeley.edu", "stemcellcenter.berkeley.edu", "bConnected.berkeley.edu", "multimedia.journalism.berkeley.edu", "cfo.berkeley.edu", "rfs.berkeley.edu", "igs.berkeley.edu", "awards.berkeley.edu", "ptolemy.berkeley.edu", "globe.berkeley.edu", "endo.berkeley.edu", "rac.berkeley.edu", "stadium.berkeley.edu", "bleex.me.berkeley.edu", "bwc.berkeley.edu", "iber.berkeley.edu", "ls-advise.berkeley.edu", "techfund.berkeley.edu", "hearstmuseum.berkeley.edu", "bioeng.berkeley.edu",
    "logic.berkeley.edu", "mvz.berkeley.edu", "ccb.berkeley.edu", "ophd.berkeley.edu", "artsdesign.berkeley.edu", "cfr.berkeley.edu", "chem.berkeley.edu", "wireless.berkeley.edu", "ucjeps.berkeley.edu", "studentcentral.berkeley.edu", "security.berkeley.edu", "software-central.berkeley.edu", "em-lab.berkeley.edu", "disability-studies.ugis.berkeley.edu", "chancellor.berkeley.edu", "advocate.berkeley.edu", "exec-ed.berkeley.edu", "taylorlab.berkeley.edu", "ies.berkeley.edu", "museums.berkeley.edu",
    "cal1card.berkeley.edu", "bcht.berkeley.edu", "fungcenter.berkeley.edu", "als.ugis.berkeley.edu", "sunsite.berkeley.edu", "builders.berkeley.edu", "thecareerplace.berkeley.edu", "asrg.berkeley.edu", "laep.ced.berkeley.edu", "arthistory.berkeley.edu", "businessservices.berkeley.edu", "bbrg.berkeley.edu", "dlab.berkeley.edu", "simons.berkeley.edu", "brand.berkeley.edu", "scr.berkeley.edu", "calmessages.berkeley.edu", "ucrc.berkeley.edu", "arts.berkeley.edu", "fischerlab.berkeley.edu",
    "kathleenryanlab.berkeley.edu", "berkeleyonline.berkeley.edu", "bitn.berkeley.edu", "math.berkeley.edu", "gradlectures.berkeley.edu", "housing.berkeley.edu", "rhetoric.berkeley.edu", "espm.berkeley.edu", "research.berkeley.edu", "upp.berkeley.edu", "outreach.berkeley.edu", "atdp.berkeley.edu", "teaching.berkeley.edu", "clsd.berkeley.edu", "cshe.berkeley.edu", "journalism.berkeley.edu", "mcb.berkeley.edu", "artstudio.berkeley.edu", "students.berkeley.edu", "socrates.berkeley.edu",
    "identity.berkeley.edu", "eureka.berkeley.edu", "calmail.berkeley.edu", "glasslab.berkeley.edu", "fss.berkeley.edu", "police.berkeley.edu", "bapps.berkeley.edu", "sexualassaultupdates.berkeley.edu", "bindery.berkeley.edu", "asc.berkeley.edu", "staffombuds.berkeley.edu", "ofew.berkeley.edu", "webaccess.berkeley.edu", "library.berkeley.edu", "optometry.berkeley.edu", "cnmat.berkeley.edu", "statistics.berkeley.edu", "lavendercal.berkeley.edu", "developer.berkeley.edu", "at.berkeley.edu",
    "asuc.berkeley.edu", "ihd.berkeley.edu", "ehs.sph.berkeley.edu", "osi.berkeley.edu", "bets.berkeley.edu", "computationalbiology.berkeley.edu", "criticaltheory.berkeley.edu", "xlab.berkeley.edu", "celtic.berkeley.edu", "astron.berkeley.edu", "tiencenter.berkeley.edu", "camps.berkeley.edu", "mentoringawards.berkeley.edu", "wisdomcafe.berkeley.edu", "planyourlegacy.berkeley.edu", "cssc.berkeley.edu", "basc.berkeley.edu", "commencement.berkeley.edu", "directory.berkeley.edu",
    "scet.berkeley.edu", "oe.berkeley.edu", "property.berkeley.edu", "controller.berkeley.edu", "calfund.berkeley.edu", "medieval.berkeley.edu", "traxlerlab.berkeley.edu", "are.berkeley.edu", "evolution.berkeley.edu", "imaging.berkeley.edu", "chess.eecs.berkeley.edu", "careercompass.berkeley.edu", "miller.berkeley.edu", "bluegold.berkeley.edu", "lma.berkeley.edu", "coeh.berkeley.edu", "ealc.berkeley.edu", "iir.berkeley.edu", "iseees.berkeley.edu", "begin.berkeley.edu", "bakerlab.berkeley.edu",
    "events.berkeley.edu", "stc.berkeley.edu", "pmb.berkeley.edu", "visitors.berkeley.edu", "creative.ugis.berkeley.edu", "urbanpolicy.berkeley.edu", "ds421.berkeley.edu", "cphs.berkeley.edu", "vcresearch.berkeley.edu", "spechtlab.berkeley.edu", "EthicsCompliance.berkeley.edu", "uroc.berkeley.edu", "sectionclub.berkeley.edu", "stsc.berkeley.edu", "geneq.berkeley.edu", "bnhm.berkeley.edu", "cads.berkeley.edu", "open.berkeley.edu", "lib.berkeley.edu", "alchemy.cchem.berkeley.edu",
    "essig.berkeley.edu", "microlab.berkeley.edu", "bspace.berkeley.edu", "polypedal.berkeley.edu", "ipira.berkeley.edu", "calrentals.housing.berkeley.edu", "bconnected.berkeley.edu", "rpgp.berkeley.edu", "folklore.berkeley.edu", "southasia.berkeley.edu", "vcei.berkeley.edu", "navyrotc.berkeley.edu", "ls.berkeley.edu", "brp.berkeley.edu", "crea.berkeley.edu", "gspp.berkeley.edu", "opa.berkeley.edu", "parents.berkeley.edu", "publicaffairs.berkeley.edu", "decal.berkeley.edu", "orias.berkeley.edu",
    "africanalumni.berkeley.edu", "supplychain.berkeley.edu", "oskicat.berkeley.edu", "summerenglish.berkeley.edu", "elsa.berkeley.edu", "ipsr.berkeley.edu", "slas.berkeley.edu", "kalx.berkeley.edu", "csac.chance.berkeley.edu", "urap.berkeley.edu", "hrweb.berkeley.edu", "biggive.berkeley.edu", "italian.berkeley.edu", "crl.berkeley.edu", "campaign.berkeley.edu", "issi.berkeley.edu", "office.chancellor.berkeley.edu", "webcast.berkeley.edu", "fbf.berkeley.edu", "vpaafw.chance.berkeley.edu",
    "engineering.berkeley.edu", "ls1.berkeley.edu", "cmes.berkeley.edu", "cseas.berkeley.edu", "cbc.berkeley.edu", "spanish-portuguese.berkeley.edu", "crlp.berkeley.edu", "nisee.berkeley.edu", "english.berkeley.edu", "deanofstudents.berkeley.edu", "complit.berkeley.edu", "crg.berkeley.edu", "compbiochem.berkeley.edu", "studentbilling.berkeley.edu", "workstudy.berkeley.edu", "academic-senate.berkeley.edu", "ptolemy.eecs.berkeley.edu", "as.ugis.berkeley.edu", "magnes.berkeley.edu",
    "publicservice.berkeley.edu", "inews.berkeley.edu", "summer.berkeley.edu", "ejce.berkeley.edu", "fma.berkeley.edu", "dsp.berkeley.edu", "tdps.berkeley.edu", "oem.berkeley.edu", "sustainability.berkeley.edu", "entrepreneurship-ecosystem.berkeley.edu", "entrepreneurship.berkeley.edu", "tebtunis.berkeley.edu", "airforcerotc.berkeley.edu", "lunchpoems.berkeley.edu", "fm.berkeley.edu", "eaop.berkeley.edu", "psychology.berkeley.edu", "administration.berkeley.edu", "potency.berkeley.edu",
    "smart.berkeley.edu", "services.housing.berkeley.edu", "nst.berkeley.edu", "eop.berkeley.edu", "youngalumni.berkeley.edu", "ahma.berkeley.edu", "tickets.berkeley.edu", "web.berkeley.edu", "french.berkeley.edu", "alumni-friends.berkeley.edu", "compliance.berkeley.edu", "blumcenter.berkeley.edu", "history.berkeley.edu", "neareastern.berkeley.edu", "german.berkeley.edu", "cgdm.berkeley.edu", "botanicalgarden.berkeley.edu", "extension.berkeley.edu", "technology.berkeley.edu", "hr.berkeley.edu",
    "pe.berkeley.edu", "legalstudies.berkeley.edu", "studyabroad.berkeley.edu", "iurd.berkeley.edu", "slavic.berkeley.edu", "vpasp.berkeley.edu", "safetrec.berkeley.edu", "mediastudies.ugis.berkeley.edu", "music.berkeley.edu", "forestry.berkeley.edu", "blc.berkeley.edu", "vcue.berkeley.edu", "evcp.chance.berkeley.edu", "lsdiscovery.berkeley.edu", "writing.berkeley.edu", "safetycounts.berkeley.edu", "themeprograms.berkeley.edu", "calnet.berkeley.edu", "bmap.berkeley.edu", "travel.berkeley.edu",
    "africa.berkeley.edu", "sspc.berkeley.edu", "redwood.berkeley.edu", "arch.ced.berkeley.edu", "clas.berkeley.edu", "bcourses.berkeley.edu", "stepbystep.berkeley.edu", "bcnm.berkeley.edu", "embedded.eecs.berkeley.edu", "internationaloffice.berkeley.edu", "funginstitute.berkeley.edu", "gsi.berkeley.edu", "tims.berkeley.edu", "clpr.berkeley.edu", "stafforg.berkeley.edu", "anthropology.berkeley.edu", "microbiology.berkeley.edu", "realestate.berkeley.edu", "classes.berkeley.edu",
    "campuspol.chance.berkeley.edu", "cstms.berkeley.edu", "privacy.berkeley.edu", "flowcytometry.berkeley.edu", "rts.berkeley.edu", "career.berkeley.edu", "reslife.berkeley.edu", "fisheritcenter.haas.berkeley.edu", "aap.berkeley.edu", "campuspol.berkeley.edu", "military.berkeley.edu", "ourenvironment.berkeley.edu", "vision.berkeley.edu", "grad.berkeley.edu", "international.berkeley.edu", "caltime.berkeley.edu", "bigideascourses.berkeley.edu", "db.cs.berkeley.edu", "mailservices.berkeley.edu",
    "calday.berkeley.edu", "glaunsingerlab.berkeley.edu", "astro.berkeley.edu", "financialaid.berkeley.edu", "cnr.berkeley.edu", "budget.berkeley.edu", "scandinavian.berkeley.edu", "gallery.berkeley.edu", "ib.berkeley.edu", "givetocal.berkeley.edu", "surf.berkeley.edu", "iastp.berkeley.edu", "homecoming.berkeley.edu", "ucdata.berkeley.edu", "skydeck.berkeley.edu", "socialwelfare.berkeley.edu", "microscopy.berkeley.edu", "callink.berkeley.edu", "lrdp.berkeley.edu", "micronet.berkeley.edu",
    "news.berkeley.edu", "environmentalsciences.berkeley.edu", "philosophy.berkeley.edu", "lewislab.berkeley.edu", "americancultures.berkeley.edu", "ethicscompliance.berkeley.edu", "ucdc.berkeley.edu", "cio.berkeley.edu", "sa.berkeley.edu", "sathercenter.berkeley.edu", "alumni.berkeley.edu", "romancelangs.berkeley.edu", "ihouse.berkeley.edu", "qb3.berkeley.edu", "vspa.berkeley.edu", "iis.berkeley.edu", "admissions.berkeley.edu", "ucjazz.berkeley.edu", "give.berkeley.edu", "desktop.berkeley.edu",
    "sda.berkeley.edu", "synbio.berkeley.edu", "erg.berkeley.edu", "facultylectures.berkeley.edu", "chpps.berkeley.edu", "calmarketplace.berkeley.edu", "best.berkeley.edu", "studentconduct.berkeley.edu", "eslibrary.berkeley.edu", "onthesamepage.berkeley.edu", "buddhiststudies.berkeley.edu", "recsports.berkeley.edu", "live-international-area-studies-academic-program.pantheon.berkeley.edu", "bcbp.berkeley.edu", "reentry.berkeley.edu", "ets.berkeley.edu", "coateslab.berkeley.edu",
    "sls.berkeley.edu", "ethnicstudies.berkeley.edu", "i4y.berkeley.edu", "access.berkeley.edu", "biology.berkeley.edu", "diversity.berkeley.edu", "sph.berkeley.edu", "bsa.berkeley.edu", "innovators.berkeley.edu", "integration-services.berkeley.edu", "ece.berkeley.edu", "cerch.berkeley.edu", "discovercal.berkeley.edu", "calanswers.berkeley.edu", "setiathome.ssl.berkeley.edu", "mud.ced.berkeley.edu", "sharedservices.berkeley.edu", "idmg.berkeley.edu", "hsp.berkeley.edu", "isf.ugis.berkeley.edu",
    "arf.berkeley.edu", "sociology.berkeley.edu", "cssr.berkeley.edu", "ucce.berkeley.edu", "nature.berkeley.edu", "bsp.berkeley.edu", "postdoc.berkeley.edu", "brie.berkeley.edu", "ugis.ls.berkeley.edu", "calswec.berkeley.edu", "trsp.berkeley.edu", "townsendcenter.berkeley.edu", "backupchildcare.berkeley.edu", "registrar.berkeley.edu", "apo.berkeley.edu", "ucpd.berkeley.edu", "classics.berkeley.edu", "sseas.berkeley.edu", "physics.berkeley.edu", "veteran.berkeley.edu", "apapps.berkeley.edu",
    "studentparents.berkeley.edu", "bancroft.berkeley.edu", "bisp.berkeley.edu", "ist.berkeley.edu", "neuroscience.berkeley.edu", "ssl.berkeley.edu", "promise.berkeley.edu", "peer.berkeley.edu", "executive.berkeley.edu", "delegations.berkeley.edu", "superfund.berkeley.edu", "orientation.berkeley.edu", "catsip.berkeley.edu", "groups.haas.berkeley.edu", "cal.berkeley.edu", "uhs.berkeley.edu", "slc.berkeley.edu", "jewishstudies.berkeley.edu", "lead.berkeley.edu", "cips.berkeley.edu",
    "srcweb.berkeley.edu", "nettools.net.berkeley.edu", "calparents.berkeley.edu", "buj.berkeley.edu", "pt.berkeley.edu", "ehs.berkeley.edu", "reunions.berkeley.edu", "academicservices.berkeley.edu", "food.berkeley.edu", "prizes-awards.ced.berkeley.edu", "pbk.berkeley.edu", "aaads.berkeley.edu", "art.berkeley.edu", "biophysics.berkeley.edu", "dutch.berkeley.edu", "blogs.berkeley.edu", "dcrp.ced.berkeley.edu", "calbandalumni.berkeley.edu", "seismo.berkeley.edu", "eml.berkeley.edu",
    "womensstudies.berkeley.edu", "150.berkeley.edu", "crsc.berkeley.edu", "lange.berkeley.edu", "fsm.berkeley.edu"])

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

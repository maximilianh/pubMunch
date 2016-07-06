# per-publisher configuration for pubCrawl.py
from os.path import *
import logging, urllib2, urlparse, urllib, re
import pubConf, pubGeneric, maxCommon, maxCommon
from collections import OrderedDict

# this file mostly is dealing with the problem that we need to figure out which journals 
# correpond to a publisher.

# it maps full publisher name to our internal publisher ID
# used for assignment of ISSNs to a directory
# links a publisher name from PubMed to a directory for the crawler

# pubPrepCrawlDir parses journal lists from various sources and groups journals
# by publishers.  In most cases, to download all journal from a publisher, all
# you have to do is to copy the publisher field from publisher.tab here and
# define a directory name for it

# format: publisher name -> directory name
# the prefix 'NLM' means that the ISSNs come from the NLM Catalog (~PubMed)
# other prefixes (WILEY, HIGHWIRE, etc) are specific journal lists
# in the tools/data directory.

crawlPubIds = {
# got a journal list from Wolter Kluwer by email
"LWW lww" : "lww",
# all ISSNs that wiley gave us go into the subdir wiley
"WILEY Wiley" : "wiley",
"WILEY EMBO" : "embo",
# we don't have ISSNs for NPG directly, so we use grouped data from NLM
"NLM Nature Publishing Group" : "npg",
"NLM American College of Chest Physicians" : "chest",
"NLM American Association for Cancer Research" : "aacr",
"NLM Mary Ann Liebert" : "mal",
"NLM Oxford University Press" : "oup",
"NLM Future Science" : "futureScience",
"NLM National Academy of Sciences" : "pnas",
"NLM American Association of Immunologists" : "aai",
"NLM Karger" : "karger",
# we got a special list of Highwire ISSNs from their website
# it needed some manual processing
# see the README.txt file in the journalList directory
"HIGHWIRE Rockefeller University Press" : "rupress",
"HIGHWIRE American Society for Microbiology" : "asm",
"HIGHWIRE Cold Spring Harbor Laboratory" : "cshlp",
"HIGHWIRE The American Society for Pharmacology and Experimental Therapeutics" : "aspet",
"HIGHWIRE American Society for Biochemistry and Molecular Biology" : "asbmb",
"HIGHWIRE Federation of American Societies for Experimental Biology" : "faseb",
"HIGHWIRE Society for Leukocyte Biology" : "slb",
"HIGHWIRE The Company of Biologists" : "cob",
"HIGHWIRE Genetics Society of America" : "genetics",
"HIGHWIRE Society for General Microbiology" : "sgm",
"NLM Informa Healthcare" : "informa",
#"Society for Molecular Biology and Evolution" : "smbe"
}

# crawler delay config, values in seconds
# these overwrite the default set with the command line switch to pubCrawl
# special case is highwire, handled in the code:
# (all EST): mo-fri: 9-5pm: 120 sec, mo-fri 5pm-9am: 10 sec, sat-sun: 5 sec (no joke) 
crawlDelays = {
    "www.nature.com"              : 5,
    "onlinelibrary.wiley.com" : 1,
    "dx.doi.org"              : 1,
    "ucelinks.cdlib.org"      : 20,
    "eutils.ncbi.nlm.nih.gov"      : 3,
    "journals.lww.com" : 0.2, # wolters kluwer
    "pdfs.journals.lww.com" : 0.2, # wolters kluwer
    "content.wkhealth.com" : 0.2, # also wolters kluwer
    "links.lww.com" : 0.2, # again wolters kluwer
    "sciencedirect.com"      : 10 # elsevier
}

def parseHighwire():
    """ create two dicts 
    printIssn -> url to pmidlookup-cgi of highwire 
    and 
    publisherName -> top-level hostnames
    >>> temps, domains = parseHighwire()
    >>> temps['0270-6474']
    u'http://www.jneurosci.org/cgi/pmidlookup?view=long&pmid=%(pmid)s'
    >>> domains["Society for Neuroscience"]
    set([u'jneurosci.org'])
    >>> domains["American Society for Biochemistry and Molecular Biology"]
    set([u'jbc.org', u'mcponline.org', u'jlr.org'])
    >>> temps["1535-9476"]
    u'http://www.mcponline.org/cgi/pmidlookup?view=long&pmid=%(pmid)s'
    """
    templates = {}
    domains = {}
    pubFname = pubConf.publisherIssnTable
    logging.info("Parsing %s to find highwire ISSNs/webservers" % pubFname)
    for row in maxCommon.iterTsvRows(pubFname):
        if not row.pubName.startswith("HIGHWIRE"):
            continue
        pubName = row.pubName.replace("HIGHWIRE ","")
        issns = [i.strip() for i in row.journalIssns.split("|")]
        servers = row.webservers.split("|")
        for issn, server in zip(issns, servers):
            template = "http://www."+server+"/cgi/pmidlookup?view=long&pmid=%(pmid)s" 
            templates[issn] = template
            domains.setdefault(pubName, set()).add(server)
            #logging.debug("HIGHWIRE CONFIG %s, %s, %s" % (pubName, template, domains[pubName]))
    return templates, domains
     
def makeHighwireConfig(domains, templates):
    " create a dict that configures a highwire publisher "
    return {
            "hostnames" : domains,
            "landingUrl_templates" : templates,
            "landingPage_errorKeywords" : "We are currently doing routine maintenance", # wait for 15 minutes and retry
            "doiUrl_replace" : {"$" : ".long"},
            "landingUrl_isFulltextKeyword" : ".long",
            "landingPage_ignoreMetaTag" : True,
            "landingUrl_pdfUrl_replace" : {"long" : "full.pdf", "abstract" : "full.pdf" },
            "landingPage_suppFileList_urlREs" : [".*/content.*suppl/DC[0-9]"],
            "suppListPage_suppFile_urlREs" : [".*/content.*suppl/.*"],
        }

def highwireConfigs():
    """ return dict publisher name -> config for all highwire publishers 
    >>> r=highwireConfigs() 
    >>> r["American Association for the Advancement of Science"]["hostnames"]
    set([u'sageke.sciencemag.org', u'sciencemag.org', u'stke.sciencemag.org'])
    >>> r["asbmb"]
    {'landingUrl_isFulltextKeyword': '.long', 'hostnames': set([u'jbc.org', u'mcponline.org', u'jlr.org']), 'landingUrl_templates': {u'0022-2275': u'http://www.mcponline.org/cgi/pmidlookup?view=long&pmid=%(pmid)s', u'1535-9476': u'http://www.jlr.org/cgi/pmidlookup?view=long&pmid=%(pmid)s', u'0021-9258': u'http://www.jbc.org/cgi/pmidlookup?view=long&pmid=%(pmid)s'}, 'landingPage_suppFileList_urlREs': ['.*/content.*suppl/DC[0-9]'], 'landingPage_errorKeywords': 'We are currently doing routine maintenance', 'landingPage_ignoreMetaTag': True, 'doiUrl_replace': {'$': '.long'}, 'landingUrl_pdfUrl_replace': {'abstract': 'full.pdf', 'long': 'full.pdf'}, 'suppListPage_suppFile_urlREs': ['.*/content.*suppl/.*']}
    """
    logging.info("Creating config for Highwire publishers")
    res = OrderedDict()
    issnTemplates, pubDomains = parseHighwire()
    # silently skip highwire parsing errors
    if issnTemplates==None:
        logging.warn("Could not read Highwire configuration")
        return {}

    # for each publisher, create a config that includes its hostnames and
    # all templates
    for pubName, domains in pubDomains.iteritems():
        #print pubName, domains
        templates = {}
        for issn, templUrl in issnTemplates.iteritems():
            for domain in domains:
                if domain in templUrl:
                    templates[issn]=templUrl
                    break
                    
        # translate long Highwire publisher name to short pubId from config file
        pubId = crawlPubIds.get("HIGHWIRE "+pubName, pubName)
        res[pubId] = makeHighwireConfig(domains, templates)
    return res

# a dict with publisherId -> configDict
confDict = None
# a dict with hostname -> publisherId
hostToPubId = None

def initConfig():
    """ define config, compile regexes and index by hostname 
    >>> initConfig()
    """
    global confDict
    global hostToPubId
    logging.info("init config")
    confDict = defineConfDict()
    confDict, hostToPubId = prepConfigIndexByHost()
    #print hostToPubId["sciencemag.org"

def defineConfDict():
    """ returns the dictionary of config statements 
    >>> d = defineConfDict()
    """
    # first setup all highwire publishers
    confDict = highwireConfigs()

    # some tweaks for certain highwire publishers

    # cshlp protocols has no pdfs
    confDict["cshlp"]["landingPage_acceptNoPdf"]=True
    confDict["cshlp"]["hostnames"].add("jb.oxfordjournals.org")


    # crawl configuration: for each website, define how to crawl the pages
    # this got more and more complicated over time. Best is to copy/paste from these examples
    # to create new ones
    # you can overwrite the automatically created ones

    confDict.update(
    {
    "pmc" :
    # not used at UCSC, we get the files via pubGetPmc/pubConvPmc, 
    # this was only a quick hack for an NIH project
    # caveats: we always keep html, can't distinguish between PDF-only and 
    # and HTML/PDF articles
    # supplementals might not always work, tested only on PLOS
    # only gets the first supplemental file
    {
        "hostnames" : ["www.ncbi.nlm.nih.gov"],
        "landingUrl_pdfUrl_append" : "pdf",
        "landingPage_hasSuppList": True,
        "landingUrl_isFulltextKeyword" : "nih.gov", # always true -> always keep fulltext
        "landingPage_suppFile_textREs" : ["Click here for additional data file.*"]
    },
    # just a quick hack, only gets PDFs, nothing else
    "American Association for the Advancement of Science":
    {
        "hostnames" : ["www.sciencemag.org"],
        "landingUrl_pdfUrl_append" : "pdf",
        "landingPage_hasSuppList": True,
        "landingUrl_isFulltextKeyword" : "sciencemag.org", # always true -> always keep fulltext
        "landingPage_stopPhrases" : ["The content you requested is not included in your institutional subscription"]
    },
    "springer" :
    # we don't use this at UCSC, we get springer directly, use at own risk
    {
        "hostnames" : ["link.springer.com"],
        #only pdfs "landingUrl_replaceREs" : {"$" : "?np=y"}, # switch sd to screen reader mode
        "landingPage_stopPhrases": ["make a payment", "purchase this article"],
        "landingPage_pdfLinkTextREs" : [".*Download PDF +\([0-9.]+ [KM]B\)"],
        "suppListPage_suppFile_urlREs" : [".*/file/MediaObjects/.*"],
        "landingPage_hasSuppList": True
    },
    "elsevier" :
    # at UCSC we don't use this, we get elsevier data via consyn.elsevier.dom
    # this is mostly for off-site use or for the odd project that doesn't
    # want to pull from consyn
    # caveats:  
    # * we cannot download text html (no function to add np=y to landing url)
    # * we don't know if we actually have access to an article 
    # * no supplemental files 
    {
        "hostnames" : ["www.sciencedirect.com"],
        #only pdfs "landingUrl_replaceREs" : {"$" : "?np=y"}, # switch sd to screen reader mode
        "landingPage_stopPhrases": ["make a payment", "purchase this article", \
            "This is a one-page preview only"],
        "landingPage_pdfLinkTextREs" : ["PDF  +\([0-9]+ K\)", "Download PDF"],
    },

    # NORMAL SITE CONFIGURATIONS AS USED AT UCSC

    # example suppinfo links 20967753 - major type of suppl, some also have "legacy" suppinfo
    # example spurious suppinfo link 8536951
    # 
    "wiley" :
    {
        "hostnames" : ["onlinelibrary.wiley.com"],
        #"landingUrl_templates" : {None: "http://onlinelibrary.wiley.com/doi/%(doi)s/full"},
        "landingUrl_templates" : {"anyIssn": "http://onlinelibrary.wiley.com/resolve/openurl?genre=article&sid=genomeBot&issn=%(printIssn)s&volume=%(vol)s&issue=%(issue)s&spage=%(firstPage)s"},
        "landingUrl_fulltextUrl_replace" : {"abstract" : "full"},
        #"doiUrl_replace" : {"abstract" : "full"},
        #"landingUrl_isFulltextKeyword" : "full",
        #"landingUrl_pdfUrl_replace" : {"full" : "pdf", "abstract" : "pdf"},
        "landingPage_suppListTextREs" : ["Supporting Information"],
        "suppListPage_suppFile_urlREs" : [".*/asset/supinfo/.*", ".*_s.pdf"],
        "suppFilesAreOffsite" : True,
        "landingPage_ignoreUrlREs"  : ["http://onlinelibrary.wiley.com/resolve/openurl.genre=journal&issn=[0-9-X]+/suppmat/"],
        "landingPage_stopPhrases" : ["You can purchase online access", "Registered Users please login"]
    },
    "lww" :
    # Lippincott Williams aka Wolters Kluwer Health
    # with suppl:
    # PMID 21617504
    # http://journals.lww.com/academicmedicine/Fulltext/2011/07000/Six_Ways_Problem_Based_Learning_Cases_Can_Sabotage.13.aspx
    # no html fulltext, no outlink from pubmed
    # PMID 9686422
    # http://journals.lww.com/psychgenetics/Abstract/1998/00820/The_Bal_I_and_Msp_I_Polymorphisms_in_the_dopamine.3.aspx
    # http://journals.lww.com/psychgenetics/Abstract/1990/01020/The_Super_Normal_Control_Group_in_Psychiatric.5.pdf
    # we cannot crawl any articles on pt.wkhealth.com

    {
        "hostnames" : ["journals.lww.com"],
        "onlyUseTemplate" : True,
        "landingUrl_templates" : {"anyIssn" : "http://content.wkhealth.com/linkback/openurl?issn=%(printIssn)s&volume=%(vol)s&issue=%(issue)s&spage=%(firstPage)s"},
        "landingPage_ignoreUrlWords" : ["issuelist"],
        #"landingUrl_fulltextUrl_replace" : {"abstract" : "fulltext"},
        "landingUrl_isFulltextKeyword" : "Fulltext", # probably not necessary, openurl goes to land page
        "landingPage_fulltextLinkTextREs" : ["View Full Text"],
        "landingPage_pdfLinkTextREs" : ["Article as PDF +\([0-9.]+ [KM]B\)"],
        "landingPage_acceptNoPdf": True,
        "landingPage_linksCanBeOffsite": True,
        "fulltextPage_hasSuppList": True,
        "suppFilesAreOffsite" : True,
        "fulltextPage_suppFile_urlREs" : ["http://links.lww.com.*"],
        "suppListPage_acceptAllFileTypes" : True,
    },


    "biokhimyia" :
    # this is their old server. post-2000 is hosted on springer
    {
        "hostnames" : ["protein.bio.msu.ru"],
        "landingUrl_isFulltextKeyword" : "full",
        "landingPage_pdfLinkTextREs" : ["Download Reprint .PDF."]
    },

    "npg" :
    # http://www.nature.com/nature/journal/v463/n7279/suppinfo/nature08696.html
    # http://www.nature.com/pr/journal/v42/n4/abs/pr19972520a.html - has no pdf
    # unusual: PMID 10854325 has a useless splash page
    {
        "hostnames" : ["www.nature.com"],
        "landingPage_stopPhrases": ["make a payment", "purchase this article"],
        "landingPage_acceptNoPdf": True,
        "landingUrl_isFulltextKeyword" : "full",
        "landingUrl_pdfUrl_replace" : {"full" : "pdf", "html" : "pdf", "abs" : "pdf"},
        "landingPage_pdfLinkTextREs" : ["Download PDF"],
        "landingUrl_suppListUrl_replace" : {"full" : "suppinfo", "abs" : "suppinfo"},
        "landingPage_suppListTextREs" : ["Supplementary information index", "[Ss]upplementary [iI]nfo", "[sS]upplementary [iI]nformation"],
        "suppListPage_suppFile_textREs" : ["[Ss]upplementary [dD]ata.*", "[Ss]upplementary [iI]nformation.*", "Supplementary [tT]able.*", "Supplementary [fF]ile.*", "Supplementary [Ff]ig.*", "Supplementary [lL]eg.*", "Download PDF file.*", "Supplementary [tT]ext.*", "Supplementary [mM]ethods.*", "Supplementary [mM]aterials.*", "Review Process File"]
    # Review process file for EMBO, see http://www.nature.com/emboj/journal/v30/n13/suppinfo/emboj2011171as1.html
    },

    # CHEST
    "chest" : {
        "hostnames" : ["journal.publications.chestnet.org"],
        # currently no access, tied to IP 128.114.50.189
    },

    # Mary-Ann Liebert:
    # with suppl
    # PMID 22017543
    # http://online.liebertpub.com/doi/full/10.1089/nat.2011.0311
    # with html
    # PMID 22145933
    # http://online.liebertpub.com/doi/abs/10.1089/aid.2011.0232
    # no html
    # PMID 7632460
    # http://online.liebertpub.com/doi/abs/10.1089/aid.1995.11.443
    "mal" :
    {
        "hostnames" : ["online.liebertpub.com"],
        "landingUrl_templates" : {"anyIssn" : "http://online.liebertpub.com/doi/full/%(doi)s"},
        "landingUrl_isFulltextKeyword" : "/full/",
        "landingUrl_pdfUrl_replace" : {"/abs/" : "/full/" },
        "landingPage_pdfLinkTextREs" : ["Full Text PDF.*"],
        "landingPage_suppListTextREs" : ["Supplementary materials.*"]
    },

    # JSTAGE: a hoster, not a publisher. Probably no permission to crawl files from here
    # not used at UCSC
    # https://www.jstage.jst.go.jp/article/circj/75/4/75_CJ-10-0798/_article
    # suppl file download does NOT work: strange javascript links
    "jstage" :
    {
        "hostnames" : ["www.jstage.jst.go.jp"],
        "landingUrl_pdfUrl_replace" : {"_article" : "_pdf" },
        "landingPage_pdfLinkTextREs" : ["Full Text PDF.*"],
        "landingPage_suppListTextREs" : ["Supplementary materials.*"]
    },
    # Rockefeller press
    # rupress tests:
    # PMID 12515824 - with integrated suppl files into main PDF
    # PMID 15824131 - with separate suppl files
    # PMID 8636223  - landing page is full (via Pubmed), abstract via DOI
    # cannot do suppl zip files like this one http://jcb.rupress.org/content/169/1/35/suppl/DC1
    # 
    "rupress" :
    {
        "hostnames" : ["rupress.org", "jcb.org"],
        "doiUrl_replace" : {"$" : ".long"},
        "landingUrl_isFulltextKeyword" : ".long",
        "landingPage_ignorePageWords" : ["From The Jcb"],
        "landingPage_ignoreMetaTag" : True,
        "landingUrl_pdfUrl_replace" : {"long" : "full.pdf", "abstract" : "full.pdf"},
        "suppListPage_addSuppFileTypes" : ["html", "htm"], # pubConf does not include htm/html
        "landingPage_pdfLinkTextREs" : ["Full Text (PDF)"],
        #"landingPage_suppListTextREs" : ["Supplemental [Mm]aterial [Iindex]", "Supplemental [Mm]aterial"],
        "landingUrl_suppListUrl_replace" : {".long" : "/suppl/DC1", ".abstract" : "/suppl/DC1"},
        "suppListPage_suppFile_textREs" : ["[Ss]upplementary [dD]ata.*", "[Ss]upplementary [iI]nformation.*", "Supplementary [tT]able.*", "Supplementary [fF]ile.*", "Supplementary [Ff]ig.*", "[ ]+Figure S[0-9]+.*", "Supplementary [lL]eg.*", "Download PDF file.*", "Supplementary [tT]ext.*", "Supplementary [mM]aterials and [mM]ethods.*", "Supplementary [mM]aterial \(.*"],
        "ignoreSuppFileLinkWords" : ["Video"],
        "ignoreSuppFileContentText" : ["Reprint (PDF) Version"],
        "suppListPage_suppFile_urlREs" : [".*/content/suppl/.*"]
    },
    # Am Society of Microbiology
    # http://jb.asm.org/content/194/16/4161.abstract = PMID 22636775
    "asm" :
    {
        "hostnames" : ["asm.org"],
        "landingUrl_isFulltextKeyword" : ".long",
        "doiUrl_replace" : {"$" : ".long"},
        "landingPage_ignoreMetaTag" : True,
        "landingPage_errorKeywords" : "We are currently doing routine maintenance", # if found on landing page Url, wait for 15 minutes and retry
        "landingUrl_pdfUrl_replace" : {"long" : "full.pdf", "abstract" : "full.pdf" },
        #"landingUrl_pdfUrl_replace" : {"long" : "full.pdf?with-ds=yes", "abstract" : "full.pdf?with-ds=yes" },
        #"landingUrl_suppListUrl_replace" : {".long" : "/suppl/DCSupplemental", ".abstract" : "/suppl/DCSupplemental"},
        "landingPage_suppFileList_urlREs" : [".*suppl/DCSupplemental"],
        "suppListPage_suppFile_urlREs" : [".*/content/suppl/.*"],
    },
    # 
    # American Assoc of Cancer Research
    # 21159627 http://cancerres.aacrjournals.org/content/70/24/10024.abstract has suppl file
    "aacr" :
    {
        "hostnames" : ["aacrjournals.org"],
        "landingUrl_templates" : {"0008-5472" : "http://cancerres.aacrjournals.org/content/%(vol)s/%(issue)s/%(firstPage)s.long"},
        "landingPage_errorKeywords" : "We are currently doing routine maintenance", # wait for 15 minutes and retry
        "doiUrl_replace" : {"$" : ".long"},
        "landingUrl_isFulltextKeyword" : ".long",
        "landingPage_ignoreMetaTag" : True,
        "landingUrl_pdfUrl_replace" : {"long" : "full.pdf", "abstract" : "full.pdf" },
        "landingPage_suppFileList_urlREs" : [".*/content[0-9/]*suppl/DC1"],
        "suppListPage_suppFile_urlREs" : [".*/content[0-9/]*suppl/.*"],
        "landingPage_stopPhrases" : ["Purchase Short-Term Access"]
    },
    # 1995 PMID 7816814 
    # 2012 PMID 22847410 has one supplement, has suppl integrated in paper
    #"cshlp" : 
    #{
        #"hostnames" : ["cshlp.org"],
        #"landingUrl_templates" : {"1355-8382" : "http://rnajournal.cshlp.org/content/%(vol)s/%(issue)s/%(firstPage)s.full"},
        #"landingPage_errorKeywords" : "We are currently doing routine maintenance", # wait for 15 minutes and retry
        #"doiUrl_replace" : {"$" : ".long"},
        #"landingUrl_isFulltextKeyword" : ".long",
        #"landingPage_ignoreMetaTag" : True,
        #"landingUrl_pdfUrl_replace" : {"long" : "full.pdf", "abstract" : "full.pdf" },
        #"landingPage_suppFileList_urlREs" : [".*/content[0-9/]*suppl/DC1"],
        #"suppListPage_suppFile_urlREs" : [".*/content[0-9/]*suppl/.*"],
        #"landingPage_stopPhrases" : ["Purchase Short-Term Access"]
    #},

    # PNAS
    "pnas" :
    {
        "hostnames" : ["pnas.org"],
        "landingUrl_templates" : {"0027-8424" : "http://pnas.org/cgi/pmidlookup?view=long&pmid=%(pmid)s"},
        "landingPage_errorKeywords" : "We are currently doing routine maintenance", # wait for 15 minutes and retry
        "doiUrl_replace" : {"$" : ".long"},
        "landingUrl_isFulltextKeyword" : ".long",
        "landingPage_ignoreMetaTag" : True,
        "landingUrl_pdfUrl_replace" : {"long" : "full.pdf", "abstract" : "full.pdf" },
        "landingPage_suppFileList_urlREs" : [".*suppl/DCSupplemental"],
        "suppListPage_suppFile_urlREs" : [".*/content/suppl/.*"],
    },
    "aspet" :
    {
        "hostnames" : ["aspetjournals.org"],
        "landingUrl_templates" : {"0022-3565" : "http://jpet.aspetjournals.org/cgi/pmidlookup?view=long&pmid=%(pmid)s"},
        "landingPage_errorKeywords" : "We are currently doing routine maintenance", # wait for 15 minutes and retry
        "doiUrl_replace" : {"$" : ".long"},
        "landingUrl_isFulltextKeyword" : ".long",
        "landingPage_ignoreMetaTag" : True,
        "landingUrl_pdfUrl_replace" : {"long" : "full.pdf", "abstract" : "full.pdf" },
        "landingPage_suppFileList_urlREs" : [".*/content[0-9/]*suppl/DC1"],
        "suppListPage_suppFile_urlREs" : [".*/content[0-9/]*suppl/.*"],
    },
    "faseb" :
    {
        "hostnames" : ["fasebj.org"],
        "landingUrl_templates" : {"0892-6638" : "http://www.fasebj.org/cgi/pmidlookup?view=long&pmid=%(pmid)s"},
        "landingPage_errorKeywords" : "We are currently doing routine maintenance", # wait for 15 minutes and retry
        "doiUrl_replace" : {"$" : ".long"},
        "landingUrl_isFulltextKeyword" : ".long",
        "landingPage_ignoreMetaTag" : True,
        "landingUrl_pdfUrl_replace" : {"long" : "full.pdf", "abstract" : "full.pdf" },
        "landingPage_suppFileList_urlREs" : [".*/content[0-9/]*suppl/DC1"],
        "suppListPage_suppFile_urlREs" : [".*/content[0-9/]*suppl/.*"],
    },
    # society of leukocyte biology
    # PMID 20971921
    "slb" :
    {
        "hostnames" : ["jleukbio.org"],
        "landingUrl_templates" : {"0741-5400" : "http://www.jleukbio.org/cgi/pmidlookup?view=long&pmid=%(pmid)s"},
        "landingPage_errorKeywords" : "We are currently doing routine maintenance", # wait for 15 minutes and retry
        "doiUrl_replace" : {"$" : ".long"},
        "landingUrl_isFulltextKeyword" : ".long",
        "landingPage_ignoreMetaTag" : True,
        "landingUrl_pdfUrl_replace" : {"long" : "full.pdf", "abstract" : "full.pdf" },
        "landingPage_suppFileList_urlREs" : [".*/content[0-9/]*suppl/DC1"],
        "suppListPage_suppFile_urlREs" : [".*/content[0-9/]*suppl/.*"],
    },
    # Company of Biologists
    "cob" :
    {
        "hostnames" : ["biologists.org"],
        "landingUrl_templates" : {"0950-1991" : "http://dev.biologists.org/cgi/pmidlookup?view=long&pmid=%(pmid)s", "0022-0949" : "http://jcs.biologists.org/cgi/pmidlookup?view=long&pmid=%(pmid)s"},
        "landingPage_errorKeywords" : "We are currently doing routine maintenance", # wait for 15 minutes and retry
        "doiUrl_replace" : {"$" : ".long"},
        "landingUrl_isFulltextKeyword" : ".long",
        "landingPage_ignoreMetaTag" : True,
        "landingUrl_pdfUrl_replace" : {"long" : "full.pdf", "abstract" : "full.pdf" },
        "landingPage_suppFileList_urlREs" : [".*/content[0-9/]*suppl/DC1"],
        "suppListPage_suppFile_urlREs" : [".*/content[0-9/]*suppl/.*"],
    },
    # Genetics Society of America
    # PMID 22714407
    "genetics" :
    {
        "hostnames" : ["genetics.org"],
        "landingUrl_templates" : {"0016-6731" : "http://genetics.org/cgi/pmidlookup?view=long&pmid=%(pmid)s"},
        "landingPage_errorKeywords" : "We are currently doing routine maintenance", # wait for 15 minutes and retry
        "doiUrl_replace" : {"$" : ".long"},
        "landingUrl_isFulltextKeyword" : ".long",
        "landingPage_ignoreMetaTag" : True,
        "landingUrl_pdfUrl_replace" : {"long" : "full.pdf", "abstract" : "full.pdf" },
        "landingPage_suppFileList_urlREs" : [".*/content[0-9/]*suppl/DC1"],
        "suppListPage_suppFile_urlREs" : [".*/content[0-9/]*suppl/.*"],
    },
    # Society of General Microbiology
    # PMID 22956734
    # THEY USE DC1 AND DC2 !!! Currently we're missing the DC1 or DC2 files... 
    # todo: invert linkdict to link -> text and not text -> link
    # otherwise we miss one link if we see twice "supplemental table" (see example)
    "sgm" :
    {
        "hostnames" : ["sgmjournals.org"],
        "landingUrl_templates" : {\
            "1466-5026" : "http://ijs.sgmjournals.org/cgi/pmidlookup?view=long&pmid=%(pmid)s", \
            "1350-0872" : "http://mic.sgmjournals.org/cgi/pmidlookup?view=long&pmid=%(pmid)s", \
            "0022-2615" : "http://jmm.sgmjournals.org/cgi/pmidlookup?view=long&pmid=%(pmid)s", \
            "0022-1317" : "http://vir.sgmjournals.org/cgi/pmidlookup?view=long&pmid=%(pmid)s"},
        "landingPage_errorKeywords" : "We are currently doing routine maintenance", # wait for 15 minutes and retry
        "doiUrl_replace" : {"$" : ".long"},
        "landingUrl_isFulltextKeyword" : ".long",
        "landingPage_ignoreMetaTag" : True,
        "landingUrl_pdfUrl_replace" : {"long" : "full.pdf", "abstract" : "full.pdf" },
        "landingPage_suppFileList_urlREs" : [".*/content.*suppl/DC[0-9]"],
        "suppListPage_suppFile_urlREs" : [".*/content.*suppl/.*"],
    },
    # SMBE - Soc of Mol Biol and Evol
    # part of OUP - careful, duplicates!
    # PMID 22956734
    "smbe" :
    {
        "hostnames" : ["mbe.oxfordjournals.org"],
        "landingUrl_templates" : \
            {"0737-4038" : "http://mbe.oxfordjournals.org/cgi/pmidlookup?view=long&pmid=%(pmid)s"},
        "landingPage_errorKeywords" : "We are currently doing routine maintenance", # wait for 15 minutes and retry
        "doiUrl_replace" : {"$" : ".long"},
        "landingUrl_isFulltextKeyword" : ".long",
        "landingPage_ignoreMetaTag" : True,
        "landingUrl_pdfUrl_replace" : {"long" : "full.pdf", "abstract" : "full.pdf" },
        "landingPage_suppFileList_urlREs" : [".*/content.*suppl/DC[0-9]"],
        "suppListPage_suppFile_urlREs" : [".*/content.*suppl/.*"],
    },
    # http://www.jimmunol.org/content/189/11/5129/suppl/DC1
    # http://www.jimmunol.org/content/suppl/2012/10/25/jimmunol.1201570.DC1/12-01570_S1-4_ed10-24.pdf
    "aai" :
    {
        "hostnames" : ["jimmunol.org"],
        "landingUrl_templates" : {"0022-1767" : "http://www.jimmunol.org/cgi/pmidlookup?view=long&pmid=%(pmid)s"},
        "landingPage_errorKeywords" : "We are currently doing routine maintenance", # wait for 15 minutes and retry
        "doiUrl_replace" : {"$" : ".long"},
        "landingUrl_isFulltextKeyword" : ".long",
        "landingPage_ignoreMetaTag" : True,
        "landingUrl_pdfUrl_replace" : {"long" : "full.pdf", "abstract" : "full.pdf" },
        "landingPage_suppFileList_urlREs" : [".*/content[0-9/]*suppl/DC1"],
        "suppListPage_suppFile_urlREs" : [".*/content/suppl/.*"],
    },
    # http://www.futuremedicine.com/doi/abs/10.2217/epi.12.21
    "futureScience" :
    {
        "hostnames" : ["futuremedicine.com", "future-science.com", "expert-reviews.com", "future-drugs.com"],
        "landingUrl_pdfUrl_replace" : {"abs" : "pdfplus"},
        "landingUrl_suppListUrl_replace" : {"abs" : "suppl"},
        "suppListPage_suppFile_urlREs" : [".*suppl_file.*"],
        "landingPage_stopPhrases" : ["single article purchase is required", "The page you have requested is unfortunately unavailable"]
    },

    }
    )
    return confDict

def compileRegexes():
    " compile regexes in confDict "
    ret = {}
    for pubId, crawlConfig in confDict.iteritems():
        newDict = {}
        for key, values in crawlConfig.iteritems():
            if key.endswith("REs"):
                newValues = []
                for regex in values:
                    newValues.append(re.compile(regex))
            else:
                newValues = values
            newDict[key] = newValues
        ret[pubId] = newDict
    return ret


def prepConfigIndexByHost():
    """ compile regexes in config and return dict publisherId -> config and hostname -> config 
    these make it possible to get the config either by hostname (for general mode)
    or by publisher (for per-publisher mode)
    >>> initConfig()
    >>> a, b = prepConfigIndexByHost()

    #>>> b["sciencemag.org"]["hostnames"]
    #>>> b["asm.org"]["hostnames"]

    #>>> a["aaas"]
    """
    compCfg = compileRegexes()
    byHost = {}
    # make sure that the last defined ones overwrite the default highwire ones
    # e.g. for aaas which deviates from the hw standard
    for pubId, crawlConfig in reversed(compCfg.items()):
    #for pubId, crawlConfig in compCfg.items():
        for host in crawlConfig.get("hostnames", []):
            byHost[host] = pubId
    #print byHost["sciencemag.org"]
    return compCfg, byHost

def printConfig():
    print ("== PUBLISHER CONFIGS ==")
    for pubName, pubConf in confDict.iteritems():
        print pubName, pubConf
        
    print ("== HOST TO PUBLISHER ASSIGNMENTS ==")
    for host, pubId in hostToPubId.iteritems():
        print("%s\t%s" % (host, pubId)) 
    
if __name__=="__main__":
    import doctest
    doctest.testmod()

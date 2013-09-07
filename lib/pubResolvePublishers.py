# routines to parse NLM Catalog and other journal lists (HIGHWIRE, wiley) and sort journals by publisher
# output to tab sep tables
# load default python packages
import logging, optparse, os, sys, collections, gzip, re, codecs, operator, glob, random
from os.path import *

# add <scriptDir>/lib/ to package search path
sys.path.insert(0, join(dirname(abspath(__file__)),"lib"))

# load our own libraries
import pubConf, pubGeneric, tabfile, maxCommon, pubPubmed
from urllib2 import urlparse

import xml.etree.cElementTree as etree

headers = "source,pIssn,eIssn,linkIssn,title,publisher,correctPublisher,urls,uniqueId,medlineTA,majMeshList,author,language,country"
Rec = collections.namedtuple("NLMRec", headers)

#PUBLISHERTAB = 'publishers.tab'
#JOURNALTAB = 'journals.tab'
#ISSNTAB = "issns.tab"
#JOURNALDIR = "_journalData"

# for some keywords in URLs, we know the right publisher
serverAssignDict = {
    "sciencedirect" : "Elsevier",
    "wiley.com" : "Wiley",
    "royalsocietypublishing.org" : "Royal Society",
    "sagepub.com" : "Sage",
    "bmj.com" : "BMJ",
    "jstage.jst.go.jp" : "Jstage",
    "springer" : "springer",
    "springerlink" : "springer",
    "scielo" : "scielo"
}

def urlStringToServers(urlString):
    " convert |-sep list of urls to list of hostnames "
    servers = set()
    urls = urlString.split("|")
    for url in urls:
        parts = urlparse.urlsplit(url)
        server = parts[1]
        server = server.replace("www.", "").strip()
        if server!="" and not "pubmedcentral" in server:
            servers.add(server)
    return servers

def recIter(tree):
    for rec in tree.findall("NCBICatalogRecord/NLMCatalogRecord"):
        #print rec
        #serial = rec.find("Serial")
        data = {}
        data["uniqueId"] = rec.find("NlmUniqueID").text
        data["title"]    = rec.find("TitleMain").find("Title").text
        medlineTa  = rec.find("MedlineTA")
        if medlineTa==None:
            logging.debug("Skipping %s" % data)
            continue

        data["medlineTA"]= medlineTa.text

        data["author"] = ""
        authorList = rec.find("AuthorList")
        if authorList!=None:
            author = authorList.find("Author")
            if author!=None:
                collName = author.find("CollectiveName")
                if collName!=None:
                    data["author"] = collName.text.strip(",. ;").replace("[etc.]","").strip(",. ;")

        pubInfo = rec.find("PublicationInfo")
        data["publisher"] = ""
        data["country"] = ""
        if pubInfo != None:
            # we assuem that the last publisher or imprint is the current one
            publishers = pubInfo.findall("Publisher")
            #print data["uniqueId"]
            #print publishers
            if publishers==None or len(publishers)==0:
                imprints = pubInfo.findall("Imprint")
                if len(imprints)==0 or imprints!=None:
                    for imprintEl in imprints:
                        publishers = imprintEl.findall("Entity")
                    if publishers==None or len(publishers)==0:
                        for imprintEl in imprints:
                            publishers = imprintEl.findall("ImprintFull")

            if publishers!=None and len(publishers)!=0:
                publisher = publishers[-1]
                pubStr = publisher.text.strip(",. ;").replace("[etc.]","").strip(",. ;")
                data["publisher"] = pubStr
            else:
                data["publisher"] = "unknown"

            if data["publisher"].lower() in \
                ["the association", "the society", "the institute", \
                 "the college", "the federation", "the department"]:
                data["publisher"]=data["author"]
            country = pubInfo.find("Country")
            if country !=None:
                data["country"] = country.text

            # <Language LangType="Primary">fre</Language>
            lang = rec.find("Language")
            if lang!=None:
                type = lang.attrib.get("LangType", "")
                if type=="Primary":
                    data["language"] = lang.text

        eloc = rec.find("ELocationList")
        urls = []
        #servers = set()
        if eloc!=None:
            elocs = eloc.findall("ELocation")
            for eloc in elocs:
                eid = eloc.find("ELocationID")
                if eid!=None and eid.attrib.get("EIdType", None)=="url":
                    url = eid.text
                    if "pubmedcentral" in url or "doi" in url:
                        logging.debug("url is PMC or DOI")
                    elif "nlm.nih.gov" in url:
                        logging.debug("url goes to pubmed")
                    elif "cdc.gov" in url:
                        logging.debug("url goes to cdc")
                    else:
                        urls.append(eid.text)
                        #parts = urlparse.urlsplit(url)
                        #server = parts[1]
                        #server = server.replace("www.", "")
                        #if server!="":
                            #servers.add(server)
        data["urls"] = "|".join(urls)
        #data["servers"] = "|".join(servers)

        majMeshes = []
        meshList = rec.find("MeshHeadingList")
        if meshList!=None:
            heads = meshList.findall("MeshHeading")
            for head in heads:
                desc = head.find("DescriptorName")
                if desc.attrib.get("MajorTopicYN", None)=="Y":
                    majMeshes.append(desc.text)
        majMesh = "|".join(majMeshes)
        data["majMeshList"] = majMesh
                
        issns = rec.findall("ISSN")
        data["eIssn"] = ""
        data["pIssn"] = ""
        if issns!=None:
            for issn in issns:
                if issn.attrib.get("IssnType", None)=="Electronic":
                    data["eIssn"]=issn.text
                if issn.attrib.get("IssnType", None)=="Print":
                    data["pIssn"]=issn.text

        if "E-only" in data["pIssn"]:
            data["pIssn"] = data["eIssn"]

        data["linkIssn"] = ""
        issnLink = rec.find("ISSNLinking")
        if issnLink!=None:
            data["linkIssn"]=issnLink.text
        else:
            #data["linkIssn"]=data["pIssn"]
            data["linkIssn"]=""
            
        data["source"] = "NLM"
        data["correctPublisher"] = ""
        row = Rec(**data)
        logging.debug("parsed XML as %s",  data)
        yield row

def writeJournals(pubGroups, outFname, headers=None, append=False, source=None):
    """ write list of records to file. If headers is specified, 
    reformat to fit into tab-sep headers. Optionally set the field source to some value."""
    logging.info("Exporting to tab-sep file %s" % outFname)
    openMode = "w"
    if append:
        openMode = "a"
    outFh = open(outFname, openMode)
    if headers==None:
        #headers = journals[0]._fields
        headers = pubGroups.values()[0][0]._fields
    if not append:
        outFh.write("\t".join(headers)+"\n")
    skipCount = 0
    Rec = collections.namedtuple("JRec", headers)
    for pubGroup, journals in pubGroups.iteritems():
        for rec in journals:
            #if rec.eIssn=="":
                #skipCount+=1
                #continue
            if headers!=None:
                recDict = rec._asdict()
                # create a new dict with all defined fields and 
                # all required fields set to "", drop all non-reqired fields
                filtRecDict = {}
                for d in recDict:
                    if d in headers:
                        filtRecDict[d] = recDict[d]
                for h in headers:
                    if h not in filtRecDict:
                        filtRecDict[h] = ""
                filtRecDict["correctPublisher"] = pubGroup
                if source:
                    filtRecDict["source"] = source
                rec = Rec(**filtRecDict)
            outFh.write((u"\t".join(rec)).encode("utf8")+"\n")

    return rec._fields
    #logging.info("Skipped %d journals without eIssn" % skipCount)

def writePubGroups(pubGroups, outFname, prefix=None, append=False):
    " write dict to tab sep file "
    openMode = "w"
    if append:
        openMode = "a"
    ofh = codecs.open(outFname, openMode, encoding="utf8")
    logging.info("Writing %s" % outFname)
    if not append:
        ofh.write("journalCount\tpubName\tpubSynonyms\ttitles\twebservers\tjournalEIssns\tjournalIssns\tuid\tcountries\tlanguages\n")
    for pubGroup, journals in pubGroups.iteritems():
        jIds = []
        jIssns = []
        syns = set()
        servers = set()
        titles = []
        uids = []
        countries = set()
        languages = set()
        for journal in journals:
            jIds.append(journal.eIssn)
            jIssns.append(journal.pIssn)
            titles.append(journal.title.replace("|"," "))
            syns.add(journal.publisher)
            jServers = urlStringToServers(journal.urls)
            servers.update(jServers)
            if "country" in journal._fields:
                countries.add(journal.country)
                uids.append(journal.uniqueId)
                languages.add(journal.language)
            #server = jourToServer.get(journal.eIssn, None)
        journalCount = len(journals)
        if prefix:
            pubGroup=prefix+" "+pubGroup
        row = [str(journalCount), pubGroup, u"|".join(syns), "|".join(titles), "|".join(servers), \
            "|".join(jIds), "|".join(jIssns), "|".join(uids), "|".join(countries), "|".join(languages)]
        ofh.write("%s\n" % "\t".join(row))

def findBestGroupForServer(pubGroups):
    " create mapping server -> best publisher (best= most journals)"
    # mapping server -> set of groups
    serverToGroups = collections.defaultdict(set)
    for groupName, journals in pubGroups.iteritems():
        for journal in journals:
            if journal.urls=="":
                continue
            jServers = urlStringToServers(journal.urls)
            for server in jServers:
                serverToGroups[server].add(groupName)

    # for each server, create list of (group, journalCount)
    # rank groups by group counts
    serverToBestGroup = {}
    for server, serverPubGroups in serverToGroups.iteritems():
        # we have some manual mappings from server -> publisher
        manualFound = False
        for serverKeyword, publisher in serverAssignDict.iteritems():
            if serverKeyword in server:
                bestGroup = publisher
                manualFound = True
                serverToBestGroup[server] = bestGroup
                break
        if manualFound:
            continue

        # now try to rank servers by journal counts
        groupCounts = []
        for group in serverPubGroups:
            groupCounts.append( (group, len(pubGroups[group])) )
        groupCounts.sort(key=operator.itemgetter(1), reverse=True)
        bestGroup, bestCount = groupCounts[0]
        if bestCount<3:
            logging.debug("Skipping %s -> %s mapping, only found in %d journals" % \
                (server, bestGroup, bestCount))
            continue
        serverToBestGroup[server] = bestGroup

    for server, bestGroup in serverToBestGroup.iteritems():
        logging.debug("%s --> %s" % (server, bestGroup))
    return serverToBestGroup
        
def regroupByServer(pubGroups, serverToBestGroup):
    " if a publisher has only one server, then assign it to the biggest group for this server "
    newGroups = {}
    for pubGroup, journals in pubGroups.iteritems():
        # get all servers of all journals
        servers = set()
        for j in journals:
            jServers = urlStringToServers(j.urls)
            servers.update(jServers)

        if len(servers)==1:
            mainServer = servers.pop()
            if mainServer!="":
                bestGroup = serverToBestGroup.get(mainServer, pubGroup)
            else:
                bestGroup = pubGroup
        else:
            bestGroup = pubGroup
        newGroups.setdefault(bestGroup, []).extend(journals)
    return newGroups

def parseTabPublisherFile(fname):
    " parse a file with columns eIssn, publisher (optional) and urls into a list of records "
    logging.info("Parsing %s" % fname)
    journals = list(maxCommon.iterTsvRows(fname, encoding="latin1"))
    # modify publisher field
    datasetName = splitext(basename(fname))[0]
    headers = list(journals[0]._fields)
    addPubField = False
    if "publisher" not in headers:
        headers.insert(0, "publisher")
        addPubField =True
    JRec = collections.namedtuple("Journal", headers)
    newJournals = []
    for j in journals:
        if j.eIssn.lower()=="print only" or j.eIssn.lower()=="unknown":
            logging.debug("Skipping journal %s, no eIssn" % j.title)
            continue
        if addPubField:
            newJ = [datasetName]
            newJ.extend(j)
            newJRec = JRec(*newJ)
        else:
            newJRec = j
        newJournals.append(newJRec)
    return newJournals

def groupPublishersByServer(journals):
    """ given a list of journal records, group similar ones based on webserver name
    return dict serverName -> (count, list of names)

    """
    # count journals per webserver
    serverCounts = collections.defaultdict(int)
    for journal in journals:
        jServers = urlStringToServers(journal.urls)
        for server in jServers:
            serverCounts[server]+=1

    # assign journal to most popular server
    # + some special cases to correct obivous errors by the NLM
    replaceServerDict = {
    "springer" : "springerlink.com",
    "springer" : "link.springer.com",
    "wiley.com" : "onlinelibrary.wiley.com",
    ".elsevier" : "sciencedirect.com"
    }

    journalGroups = {}
    # for all servers of a journal, only keep the highest ranked (=number of journals) one
    for journal in journals:
        servers = urlStringToServers(journal.urls)

        jServerCounts = []
        for server in servers:
            jServerCounts.append((server, serverCounts[server]))
        jServerCounts.sort(key=operator.itemgetter(1), reverse=True)
        topServer = jServerCounts[0][0]
        for replFrom, replTo in replaceServerDict.iteritems():
            if replFrom in topServer:
                topServer = replTo
                break
        journalGroups.setdefault(topServer, []).append(journal.publisher)

    # count and return
    ret = {}
    for server, pubList in journalGroups.iteritems():
        count = len(pubList)
        pubSet = set(pubList)
        ret[server] = (count, pubSet)
    return ret

def groupPublishersByName(journals):
    """ given a list of journal records, group similar ones based on some heuristics,
    return dict (groupName) -> (list of journal records)
    Heuristics are using: URL, then ISSN prefix, then name
    >>> class D: pass
    >>> j1 = D()
    >>> j1.urls= "http://rsx.sagepub.com/archive/"
    >>> j1.issn = "1381-1991"
    >>> j1.publisher = "elsevier"
    >>> groupPublishersByName([j1]).keys()
    ['Sage']
    """
    # make dict with publisher -> count
    pubCountDict = collections.defaultdict(int)

    # remove these words from publishers before grouping
    removeWords = "Press,Verlag,Services,Inc,Incorporated,AG,Publications,Journals,Editiones,Asia,Ltd,Media,Publishers,International,Group,Publishing,Pub ,Pub.,Periodicals,Pub,Limited,Co,Pvt".split(",")

    pubIssnPrefix = {
    "10.1016" : "elsevier",
    "10.1006" : "elsevier",
    "10.1157" : "elsevier",
    "10.3182" : "elsevier",
    "10.1067" : "elsevier",
    "10.1078" : "elsevier",
    "10.1053" : "elsevier",
    "10.1054" : "elsevier",
    "10.1251" : "springer",
    "10.1245" : "springer",
    "10.1617" : "springer",
    "10.1891" : "springer",
    "10.1140" : "springer",
    "10.1007" : "springer",
    }

    # some manual rules for grouping, to force to a given final publisher if a certain keyword is found 
    # if KEY is part of publisher name, publisher is grouped into VAL
    pubReplaceDict = {
        "Academic Press" : "Elsevier",
        "Elsever":"Elsevier", \
        "Elsevier":"Elsevier", \
        "Nature":"Nature Publishing Group", \
        "Thieme":"Thieme", \
        "Springer":"Springer", \
        "blackwell":"Wiley",
        "Wiley":"Wiley",
        "munksgaard":"Wiley",
        "humana":"Springer",
        "hindawi" : "Hindawi",
        "sage" : "Sage",
        "Kluwer" : "Wolters Kluwer",
        "Adis" : "ADIS",
        "ADIS" : "ADIS",
        "adis" : "ADIS",
        "de Gruyter" : "de Gruyter",
        "Williams and Wilkins" : "Wolters Kluwer",
        "Chicago" : "University Of Chicago",
        "Mosby" : "Elsevier",
        "Masson" : "Elsevier",
        "Cell Press" : "Elsevier",
        "Churchill" : "Elsevier",
        "cambridge" : "Cambridge Univ. Press",
        "karger" : "Karger",
        "pergamon" : "Elsevier",
        "british medical" : "BMJ Group",
        "lippincott" : "Wolters Kluwer",
        "Royal Society of Medicine" : "Royal Society of Medicine",
        "VCH Verlag" : "Wiley",
        "taylor" : "Informa",
        "american physical society" : "American Physical Society",
        "ieee" : "IEEE",
        "bmj" : "BMJ Group",
        "oxford university" : "Oxford University Press",
        "oxford journals" : "Oxford University Press",
        "saunders" : "WB Saunders",
        "American Institute of Physics" : "American Institute of Physics",
        "Churchill Livingstone" : "Churchill Livingstone",
        "Portland" : "Portland Press",
        "Rockefeller University" : "Rockefeller University Press",
        "lancet" : "Elsevier",
        "WB Saunders" : "Elsevier",
        "schattauer" : "FK Schattauer",
        "Future" : "Future Science",
        "Expert Reviews" : "Future Science"
    }

    # group publishers together
    pubDict = {}
    for journal in journals:
        publisher = journal.publisher
        resolved = False

        # first try to use the urls to map to publishers
        jServers = urlStringToServers(journal.urls)
        #print journal.publisher, jServers
        for jServer in jServers:
            for server, serverPub in serverAssignDict.iteritems():
                #print jServer, server, serverPub
                if server in jServer:
                    #print "found", server, serverPub
                    pubGroup = serverPub
                    resolved = True
                    break
            if resolved:
                break

        # then try the issn prefix
        if not resolved:
            for issnPrefix, issnPub in pubIssnPrefix.iteritems():
                if journal.pIssn.startswith(issnPrefix) or journal.eIssn.startswith(issnPrefix):
                    pubGroup = issnPub
                    resolved = True

        # then try the name
        if not resolved:
            pubGroup = publisher.strip()
            pubGroup = pubGroup.replace(" &"," and").replace(",","").replace(".","").replace("-", " ")
            pubGroup = pubGroup.replace("Assn", "Association")
            pubGroup = pubGroup.replace("Of", "of")
            pubGroup = pubGroup.replace("Dept", "Department")
            pubGroup = pubGroup.replace("U S ", "US")
            pubGroup = pubGroup.replace("Univ ", "University ")
            pubGroup = pubGroup.replace("Univ. ", "University ")
            pubGroup = pubGroup.replace('"', "")

            # first try with manual groupings
            for pubShort, pubName in pubReplaceDict.iteritems():
                if pubShort.lower() in pubGroup.lower():
                    pubGroup = pubName
                    resolved = True
                    break

            # if this doesn't work, remove some words and try manual groupings again
            if not resolved:
                for word in removeWords:
                    pubGroup = re.sub("(^| )%s($| )" % word, " ", pubGroup)
                    pubGroup = pubGroup.strip(" ,.;[]()")

                for pubShort, pubName in pubReplaceDict.iteritems():
                    if pubShort.lower() in pubGroup.lower():
                        pubGroup = pubName
                        break

        pubDict.setdefault(pubGroup, []).append(journal)

    return pubDict

def parseNlmCatalog(inFname):
    " convert NLM's XML format to a tab-sep file and return a dict publisher -> journalCount "
    if inFname.endswith(".gz"):
        data = gzip.open(inFname).read()
    else:
        data = open(inFname).read()
        
    logging.info("Parsing XML file %s into memory" % inFname)
    #data = "<nlm>"+data+"</nlm>"
    tree = etree.fromstring(data)
    journals = list(recIter(tree))
    return journals

def journalToBestWebserver(journals):
    """ given a list of journal records create a mapping journal -> webserver 
    We will assign a journal to the biggest webserver, if there are several ones """
    # count journals per webserver
    serverCounts = collections.defaultdict(int)
    for journal in journals:
        jServers = urlStringToServers(journal.urls)
        for server in jServers:
            serverCounts[server]+=1

    # assign journal to most popular server
    # + some special cases to correct obivous error by the NLM

    replaceServerDict = {
    "springer" : "springerlink.com",
    "wiley.com" : "onlinelibrary.wiley.com",
    ".elsevier" : "sciencedirect.com"
    }

    journalToServer = {}
    for journal in journals:
        servers = urlStringToServers(journal.urls)
        jServerCounts = []
        for server in servers:
            jServerCounts.append((server, serverCounts[server]))
        jServerCounts.sort(key=operator.itemgetter(1), reverse=True)
        topServer = jServerCounts[0][0]
        for replFrom, replTo in replaceServerDict.iteritems():
            if replFrom in topServer:
                topServer = replTo
                break
        journalToServer[journal.eIssn] = topServer
    return journalToServer

def convertNlmAndTab(nlmCatalogFname, tabSepFnames, journalFname, pubFname):
    """ init outDir by parsing journal list files. generate journal and publisher 
        tables from it.
    """

    # process NLM xml file
    journals = parseNlmCatalog(nlmCatalogFname)
    pubGroups = groupPublishersByName(journals)
    serverToBestGroup = findBestGroupForServer(pubGroups)
    pubGroups = regroupByServer(pubGroups, serverToBestGroup)
    writePubGroups(pubGroups, pubFname, prefix="NLM")
    headers = writeJournals(pubGroups, journalFname, source="NLM")

    # integrate tab-sep files received from other publishers
    for tabSepFname in tabSepFnames:
        datasetName = splitext(basename(tabSepFname))[0].upper()
        journals = parseTabPublisherFile(tabSepFname)
        pubGroups = groupPublishersByName(journals)
        writePubGroups(pubGroups, pubFname, \
                prefix=datasetName, append=True)
        writeJournals(pubGroups, journalFname, headers, append=True, source=datasetName)

def initJournalDir(journalInDir, journalDataDir, nlmCatalogFname, journalFname, pubFname):
    " fill the journal data dir pubConf.journalData with two tab sep files "
    if not isdir(journalDataDir):
        logging.info("Creating %s" % journalDataDir)
        os.makedirs(journalDataDir)

    listDir = journalInDir
    logging.info("importing journal info from %s" % listDir)

    if nlmCatalogFname==None:
        nlmCatalogFname = join(listDir, "nlmCatalog.English.xml.gz")

    otherTabFnames = glob.glob(join(listDir, "*.tab"))
    convertNlmAndTab(nlmCatalogFname, otherTabFnames, journalFname, pubFname)
    
if __name__=="__main__":
    import doctest
    doctest.testmod()

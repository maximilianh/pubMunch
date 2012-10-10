# package to download and parse xml files from pubmed/medline
# + some functions to query eutils

import logging, urllib2, pubConf, maxXml, pubStore, re, time, urllib, traceback, httplib
from xml.etree.ElementTree import ParseError
from xml.etree.cElementTree import ParseError as ParseError2

class PubmedError(Exception):
    def __init__(self, longMsg, logMsg, detailMsg=None):
        self.longMsg = longMsg
        self.logMsg = logMsg
        self.detailMsg = detailMsg

    def __str__(self):
        return repr(self.longMsg+"/"+self.logMsg)

def parseMedline(xmlParser):
    """
    fill article data dict with pubmed xml data

    >>> xml = PubmedTestDoc()
    >>> list(parseMedline(maxXml.XmlParser(string=xml)))
    [{'pii': 'awq087', 'doi': '10.1093/brain/awq087', 'day-pubmed': '31', 'month-pubmed': '05', 'title': 'Tyrosine hydroxylase deficiency: a treatable disorder of brain catecholamine biosynthesis.', 'vol': '133', 'meshHeadings': 'Age of Onset,Useless Research', 'abstract': 'An infantile onset, progressive, hypokinetic-rigid syndrome with dystonia (type A), and a complex encephalopathy with neonatal onset (type B). Decreased cerebrospinal fluid concentrations of homovanillic acid and c.698G>A and c.707T>C mutations. Carriership of at least one promotor mutation, however, apparently predicts type A tyrosine hydroxylase deficiency. Most patients with tyrosine hydroxylase deficiency can be successfully treated with l-dopa.', 'authors': 'Willemsen MA, Verbeek MM', 'pmcId': '', 'affiliation': 'Radboud University Nijmegen Medical Centre, Donders Institute for Brain, Cognition and Behaviour, Department of Paediatric Neurology (820 IKNC), PO Box 9101, 6500 HB Nijmegen, The Netherlands. m.willemsen@cukz.umcn.nl', 'year-pubmed': '2010', 'articleType': 'research-article', 'pubmedArticleTypes': "Journal Article,Research Support, Non-U.S. Gov't", 'year': '2010', 'pmid': '20430833', 'eIssn': '1460-2156', 'issue': 'Pt 6', 'journal': 'Brain : a journal of neurology', 'fulltextUrl': 'http://www.ncbi.nlm.nih.gov/pmc/articles/PMC/', 'printIssn': '1460-1234'}]
    """
    data = pubStore.createEmptyArticleDict()
    #medlineData           = xmlParser.getXmlFirst("MedlineCitation")
    medlineData           = xmlParser
    data["pmid"]          = medlineData.getTextFirst("PMID")
    data["externalId"]            = "PMID"+data["pmid"]
    data["fulltextUrl"]   = "http://www.ncbi.nlm.nih.gov/pubmed/%s" % data["pmid"]
    logging.log(5, "PMID %s" % data["pmid"])
    #data["year-pubmed"]   = medlineData.getTextFirst("DateCreated/Year")
    #data["month-pubmed"]  = medlineData.getTextFirst("DateCreated/Month")
    #data["day-pubmed"]    = medlineData.getTextFirst("DateCreated/Day")
    otherIds         = medlineData.getTextAll("OtherID", reqAttrDict={"Source":"NLM"})
    pmcIds = [i for i in otherIds if i.startswith("PMC")]
    if len(pmcIds) > 0:
        data["pmcId"] = pmcIds[0].split()[0].replace("PMC","")

    artTree               = medlineData.getXmlFirst("Article")
    data["title"]         = artTree.getTextFirst("ArticleTitle", default="")

    # handle structured abstracts
    abstractParts = []
    abstractTrees         = artTree.getXmlAll("Abstract/AbstractText")
    for aEl in abstractTrees:
        label = aEl.getAttr("NlmCategory")
        abstract = ""
        if label!=None:
            abstract = "<p>%s</p> " % label
        abstract += aEl.getText()
        abstractParts.append(abstract)
    data["abstract"]      = "".join(abstractParts)

    if data["abstract"]=="":
        data["abstract"]      = artTree.getTextFirst("OtherAbstract/AbstractText", default="")

    data["authorAffiliations"]   = artTree.getTextFirst("Affiliation", default="")
    data["doi"]           = artTree.getTextFirst("ELocationID", default="", reqAttrDict={"EIdType":"doi"})

    data["journalUniqueId"] = medlineData.getTextFirst("MedlineJournalInfo/NlmUniqueID")
    linkingIssn = medlineData.getTextFirst("MedlineJournalInfo/ISSNLinking")
    
    journalTree = artTree.getXmlFirst("Journal")
    data["eIssn"]       = journalTree.getTextFirst("ISSN", reqAttrDict={"IssnType": 'Electronic'}, default="")
    data["printIssn"]   = journalTree.getTextFirst("ISSN", reqAttrDict={"IssnType": 'Print'}, default="")
    if linkingIssn!=None:
        data["eIssn"]       = linkingIssn
        data["printIssn"]   = linkingIssn
        
    data["vol"]         = journalTree.getTextFirst("JournalIssue/Volume", default="")
    data["issue"]       = journalTree.getTextFirst("JournalIssue/Issue", default="")
    data["year"]        = journalTree.getTextFirst("JournalIssue/PubDate/Year", default="")
    data["journal"]     = journalTree.getTextFirst("Title", default="")

    authorList  = artTree.getXmlFirst("AuthorList")
    lastNames   = []
    initialList = []
    if authorList!=None:
        authorTrees = authorList.getXmlAll("Author")
        for authorTree in authorTrees:
            lastName = authorTree.getTextFirst("LastName", default="")
            if lastName=="":
                lastName = authorTree.getTextFirst("CollectiveName", default="")
            lastNames.append(lastName)

            initials = authorTree.getTextFirst("ForeName", default="")
            if initials=="":
                initials = authorTree.getTextFirst("Initials", default="")
            initialList.append(initials)

    authors = [lastNames[i]+", "+initialList[i] for i in range(0, min(len(lastNames), len(initialList)))]
    data["authors"]="; ".join(authors)

    articleTypeList = artTree.getTextAll("PublicationTypeList/PublicationType")
    articleTypesString  = ",".join(articleTypeList)

    articleType="research-article"

    if "Review" in articleTypeList:
       articleType = "review"
    if "letter" in articleTypeList:
       articleType = "research-article"

    noResearchArticleTags = ["Bibliography", "Biography", 
        "Case Reports", "Webcasts",  
        "Dictionary", "Directory",
        "Editorial", "Festschrift",
        "Patient Education Handout", "Periodical Index", 
        "Portraits", "Published Erratum", "Scientific Integrity Review"
        "Congresses"]

    for noResearchArticleTag in noResearchArticleTags:
        if noResearchArticleTag in articleTypeList:
            articleType = "other"

    data["articleType"]        = articleType
    #data["pubmedArticleTypes"] = articleTypesString

    logging.log(5, "pubmedArticleTypes %s, articleType %s" % (articleTypesString, articleType))

    meshDescriptors = []
    meshHeadingList       = medlineData.getXmlFirst("MeshHeadingList", default="")
    if meshHeadingList:
        #for meshHeadingDescriptor in meshHeadingList.getTextAll("MeshHeading/DescriptorName", reqAttrDict={"MajorTopicYN":"Y"}):
        for meshHeadingDescriptor in meshHeadingList.getTextAll("MeshHeading/DescriptorName"):
            meshDescriptors.append(meshHeadingDescriptor.strip())

    data["keywords"] = "/".join(meshDescriptors)

    return data

def parsePubmedFields(xmlEl, dataDict):
    """ parse special pubmed (not medline) fields, like doi, pmc, etc """
    dataDict["doi"]           = xmlEl.getTextFirst("ArticleIdList/ArticleId", reqAttrDict = {"IdType" : 'doi'}, default="")
    dataDict["pmcId"]         = xmlEl.getTextFirst("ArticleIdList/ArticleId", reqAttrDict = {"IdType" : 'pmc'}, default="").replace("PMC", "")
    return dataDict

def parsePubmedMedlineIter(xml, fromMedline=False):
    """
    Parse pubmed xml format and yield as dictionary, see parseMedline
    records come either from Pubmed as a <PubmedArticleSet><PubmedArticle><MedlineCitation>...
    or from Medline as <MedlineCitationSet><MedlineCitation>...</MedlineCitation>
    """
    if fromMedline:
        recordTag = "MedlineCitation"
        closeTag = "</MedlineArticleSet>"
        openTag = "<MedlineArticleSet"
    else:
        recordTag = "PubmedArticle/MedlineCitation"
        closeTag = "</PubmedArticleSet>"
        openTag = "<PubmedArticleSet>"
        # NCBI eutils sometimes "forgets" the opening/closing tags
        if xml.strip()=="":
            logging.error("Got empty XML file from NCBI")
            raise PubmedError("Got empty XML from NCBI", "pubmedEmptyXml")

        if not openTag in xml:
            logging.warn("Addding opening tag")
            xml = openTag + "\n" + xml

        if not closeTag in xml:
            logging.warn("Addding closing tag")
            xml = xml+"\n"+closeTag

    logging.debug("Parsing pubmed file")
    try:
        topEl       = maxXml.XmlParser(string=xml)
    except ParseError:
        logging.debug("Error on parsing this XML: %s" % xml)
        raise
    except ParseError2:
        logging.debug("Error on parsing this XML: %s" % xml)
        raise

    for medlineCitEl in topEl.getXmlAll(recordTag):
        dataDict = parseMedline(medlineCitEl)
        if not fromMedline:
            pubmedCitEl = topEl.getXmlFirst("PubmedArticle/PubmedData")
            dataDict = parsePubmedFields(pubmedCitEl, dataDict)
        yield dataDict

def ncbiEFetchGenerator(ids, dbName="pubmed", tool="pubtools", email=pubConf.email, debug=False):
    """
    retrieve records in xml format from ncbi, and yield as dictionaries
    >> print len(list(ncbiEFetchGenerator(["9322214"], debug=True)))
    1
    """
    idsLeft=list(set(ids))
    retmax = 500

    downloadCount=0
    while len(idsLeft)!=0:
        retStart = downloadCount
        downloadIds = idsLeft[:min(retmax, len(idsLeft))]
        logging.debug("Getting data on %d PMIDs from NCBI, %d PMIDs left to download" % (len(downloadIds), len(idsLeft)))
        url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=%s&tool=%s&email=%s&retmode=xml&rettype=medline&id=%s" % (dbName, tool, email, ",".join(downloadIds))
        logging.debug("Getting %s" % url)

        tryCount = 0
        xml = None
        while tryCount < 10 and xml == None:
            try:
                tryCount += 1
                xml = urllib2.urlopen(url).read()
                if xml == None:
                    raise Exception("Could not connect to Eutils")

                idsLeft= list(set(idsLeft).difference(downloadIds))
                for pubmedData in parsePubmedMedlineIter(xml):
                    downloadCount+=1
                    yield pubmedData
            #except urllib2.HTTPError:
                ##I sometimes see "HTTP Error 502: Bad Gateway"
                #logging.info("HTTP Error on eutils, pausing for 120 secs")
                #time.sleep(120)
            except urllib2.URLError: # this should handle HTTPError, too
                logging.info("HTTP Error on eutils, pausing for 120 secs")
                time.sleep(120)
            except httplib.BadStatusLine:
                logging.info("Bad status line on eutils, pausing 120 secs")
                time.sleep(120)
            except ParseError:
                logging.info("XML ParseError on eUtils, pausing 120 secs")
                time.sleep(120)

def ncbiESearch(query, dbName="pubmed", tool="", email="maximilianh@gmail.com", debug=False, delaySecs=0):
    """ retrieve pmids for query, returns list of ids 
    >> len(set(ncbiESearch("human genome", debug=True)))
    66009
    """
    if debug:
        logging.getLogger().setLevel(5)
        logging.debug("debug mode activated")

    retmax=100000
    addString=""
    query = urllib.quote(query)

    idsLeft = None
    allPmids = []

    while idsLeft>0 or idsLeft==None:
        logging.debug( "PMIDs left %s, PMIDs downloaded %d" % (str(idsLeft), len(allPmids)))
        url = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=%s&tool=retrPubmed&tool=%s&email=%s&term=%s&retstart=%d&retmax=%d%s' % (dbName, tool, email, query, len(allPmids), retmax, addString)
        req = urllib2.Request(url)
        html = urllib2.urlopen(req)
        logging.debug("Getting "+url+"\n")
        
        pmids = []
        for line in html:
            if line.find("<Count>")!=-1:
                if idsLeft==None: # only use the first "count" line
                    fs = re.split("<.{0,1}Count>", line)
                    idsLeft = int(fs[1])
            if line.find("<Id>")!=-1:
                pmid = line.strip().replace("<Id>","").replace("</Id>", "")
                pmids.append(pmid)
        idsLeft -= len(pmids)
        allPmids.extend(pmids)
        logging.info("Sleeping %d seconds" % delaySecs)
        time.sleep(delaySecs)

    return allPmids

def PubmedTestDoc():
    return '''
        <PubmedArticle> 
        <MedlineCitation Owner="NLM" Status="MEDLINE">  
        <PMID>20430833</PMID>
        <DateCreated>
            <Year>2010</Year>
            <Month>05</Month>
            <Day>31</Day>
        </DateCreated>
        <DateCompleted>
            <Year>2010</Year>
            <Month>06</Month>
            <Day>22</Day>
        </DateCompleted>
        <Article PubModel="Print-Electronic">
            <Journal>
                <ISSN IssnType="Print">1460-1234</ISSN>
                <ISSN IssnType="Electronic">1460-2156</ISSN>
                <JournalIssue CitedMedium="Internet">
                    <Volume>133</Volume>
                    <Issue>Pt 6</Issue>
                    <PubDate>
                        <Year>2010</Year>
                        <Month>Jun</Month>
                    </PubDate>
                </JournalIssue>
                <Title>Brain : a journal of neurology</Title>
                <ISOAbbreviation>Brain</ISOAbbreviation>
            </Journal>
            <ArticleTitle>Tyrosine hydroxylase deficiency: a treatable disorder of brain catecholamine biosynthesis.</ArticleTitle>
            <Pagination>
                <MedlinePgn>1810-22</MedlinePgn>
            </Pagination>
            <Abstract>
                <AbstractText>An infantile onset, progressive, hypokinetic-rigid syndrome with dystonia (type A), and a complex encephalopathy with neonatal onset (type B). Decreased cerebrospinal fluid concentrations of homovanillic acid and c.698G&gt;A and c.707T&gt;C mutations. Carriership of at least one promotor mutation, however, apparently predicts type A tyrosine hydroxylase deficiency. Most patients with tyrosine hydroxylase deficiency can be successfully treated with l-dopa.</AbstractText>
            </Abstract>
            <Affiliation>Radboud University Nijmegen Medical Centre, Donders Institute for Brain, Cognition and Behaviour, Department of Paediatric Neurology (820 IKNC), PO Box 9101, 6500 HB Nijmegen, The Netherlands. m.willemsen@cukz.umcn.nl</Affiliation>
            <AuthorList CompleteYN="Y">
                <Author ValidYN="Y">
                    <LastName>Willemsen</LastName>
                    <ForeName>Mich&#233;l A</ForeName>
                    <Initials>MA</Initials>
                </Author>
                <Author ValidYN="Y">
                    <LastName>Verbeek</LastName>
                    <ForeName>Marcel M</ForeName>
                    <Initials>MM</Initials>
                </Author> 
            </AuthorList>
            <Language>eng</Language>
            <PublicationTypeList>
                <PublicationType>Journal Article</PublicationType>
                <PublicationType>Research Support, Non-U.S. Gov't</PublicationType>
            </PublicationTypeList>
            <ArticleDate DateType="Electronic">
                <Year>2010</Year>
                <Month>04</Month>
                <Day>29</Day>
            </ArticleDate>
        </Article>
        <MedlineJournalInfo>
            <Country>England</Country>
            <MedlineTA>Brain</MedlineTA>
            <NlmUniqueID>0372537</NlmUniqueID>
            <ISSNLinking>0006-8950</ISSNLinking>
        </MedlineJournalInfo>
        <ChemicalList>
            <Chemical>
                <RegistryNumber>0</RegistryNumber>
                <NameOfSubstance>Catecholamines</NameOfSubstance>
            </Chemical>
        </ChemicalList>
        <CitationSubset>AIM</CitationSubset>
        <CitationSubset>IM</CitationSubset>
        <MeshHeadingList>
            <MeshHeading>
                <DescriptorName MajorTopicYN="N">Age of Onset</DescriptorName>
            </MeshHeading>
            <MeshHeading>
                <DescriptorName MajorTopicYN="Y">Useless Research</DescriptorName>
            </MeshHeading>
        </MeshHeadingList>
        </MedlineCitation>  
        <PubmedData>
           <History>
            <PubMedPubDate PubStatus="aheadofprint">
                <Year>2010</Year>
                <Month>4</Month>
                <Day>29</Day>
            </PubMedPubDate>
            <PubMedPubDate PubStatus="pubmed">
                <Year>2010</Year>
                <Month>5</Month>
                <Day>1</Day>
                <Hour>6</Hour>
                <Minute>0</Minute>
            </PubMedPubDate>
            <PubMedPubDate PubStatus="medline">
                <Year>2010</Year>
                <Month>6</Month>
                <Day>23</Day>
                <Hour>6</Hour>
                <Minute>0</Minute>
            </PubMedPubDate>
        </History>
        <PublicationStatus>ppublish</PublicationStatus>
        <ArticleIdList>
            <ArticleId IdType="pii">awq087</ArticleId>
            <ArticleId IdType="doi">10.1093/brain/awq087</ArticleId>
            <ArticleId IdType="pubmed">20430833</ArticleId>
        </ArticleIdList>
        </PubmedData>
        </PubmedArticle>'''

def getOnePmid(pmid):
    " fill out article data dict via eutils service for one single pmid "
    articleDataList = list(ncbiEFetchGenerator([pmid]))
    if len(articleDataList)==0:
        logging.info("No data from Pubmed for PMID %s" % str(pmid))
        return None
    else:
        pubMeta = articleDataList[0]
        logging.log(5, pubMeta)
        return pubMeta

def stripTag(line):
    remHtml = re.compile("<(.|\n)*?>")
    line = re.sub(remHtml, "", line)
    line = line.strip()
    return line

def getOutlinks(pmid):
    """ use NCBI eutils to get outlinks for a pmid as a list """
    logging.debug("%s: Getting outlink from pubmed" % (pmid))
    url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id=%s&retmode=llinks&cmd=llinks" % pmid
    #try:
    req = urllib2.Request(url)
    #except
    #logging.info(traceback.format_exc())
    #logging.info("Exception when downloading")
    #return None
    req.add_header('User-Agent', 'User-Agent: Mozilla (max@soe.ucsc.edu, http://text.soe.ucsc.edu)')
    html = urllib2.urlopen(req)
    outlinks = {}
    provider = False
    fullText = False

    for line in html:
        if line.find("<ObjUrl>") != -1:
            url=""
            fullText=False
            origPublisher=False
        if line.find("Attribute") != -1:
            attribute=stripTag(line)
            if attribute.lower()=="full-text online" or attribute=="full-text pdf":
                fullText=True
        if line.find("<NameAbbr>") != -1 and fullText and origPublisher:
            db = stripTag(line)
            outlinks[db]=url
        if line.find("publishers/providers")!=-1:
            origPublisher=True
        if line.find("<Provider>") != -1:
            provider=True
        if line.find("</Provider>") != -1:
            provider=False
        if line.find("<DbFrom>") != -1:
            db = stripTag(line)
        if line.find("<Url>") != -1 and not provider:
            url = line
            url = stripTag(url).replace("&amp;", "&") # XX strange!
            url = stripTag(url).replace("&lt;", "<")
            url = stripTag(url).replace("&gt;", ">")
    logging.debug("%s: Found outlinks %s" % (pmid, str(outlinks)))
    return outlinks

if __name__=="__main__":
    import doctest
    doctest.testmod()


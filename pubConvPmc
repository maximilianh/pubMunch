#!/usr/bin/env python

# default python packages
import sys, logging, optparse, os, collections, tarfile, mimetypes, csv, gzip, math, glob, tempfile, shutil
from os.path import *
from collections import namedtuple

# add <scriptDir>/lib/ to package search path
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "lib")
sys.path.insert(0, pubToolsLibDir)

# now load our own libraries
import pubGeneric, maxRun, pubStore, pubConf, maxCommon
from pubXml import *

MAXFILESIZE = 50000000 # don't even try to convert files bigger than 50MB

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [options] <in> <out> - convert a local mirror of the Pubmed Central FTP server to pubtools format

Example:
    pubConvPmc /hive/data/outside/pubs/pmc/ /hive/data/inside/pubs/text/pmc/

If in and out are directories:
    The file_list.csv will be split into x chunks, written to
    file_list.split/xxx.csv and jobs submitted to process all of these 
    Will create one .zip output file per chunk in the output directory
If in and out are files:
    Will parse the input file as a file_list.txt file 
    Writes to outDir.articleInfo/.fileInfo/.fileArticles

to debug xml parser: just specify one xml file, will output to stdout
""")

parser.add_option("", "--chunkCount", dest="chunkCount", action="store", type="int", help="number of chunks to create, default %default", default=2000)
parser.add_option("", "--minId", dest="minId", action="store", help="numerical IDs written to the pubStore start at this number times one billion to prevent overlaps of numerical IDs between publishers, default %s", default=pubConf.identifierStart["pmc"]) 
parser.add_option("", "--dryRun", dest="dryRun", action="store_true", help="do not submit any cluster jobs, just print commands or create jobList file", default=False)
#parser.add_option("", "--notCompress", dest="notCompress", action="store_true", help="do not use compression", default=False) 
parser.add_option("-u", "--updateDb", dest="updateDb", action="store_true", help="only export data to sqlite db defined in pubConf")
parser.add_option("", "--auto", dest="auto", action="store_true", help="set input and output dirs based on pubConf directories")

pubGeneric.addGeneralOptions(parser, logDir=True, keepTemp=True)
(options, args) = parser.parse_args()


# ==== FUNCTIONs =====
def metaInfoFromTree(metaInfo, field, etree, path, convertToAscii=False, reqAttrName=None, reqAttrValue=None, squeak=True, isAbstract=False):
    """ helper function for parsePmcXml: get text of xml tree given path
    expression, put into metaInfo[field] if path matches several elements,
    process only the the on where the attribute reqAttrName has the value
    reqAttrValue
    
    strips all newslines and tabs of anything returned
    """ 

    element = findChild(etree, path, convertToAscii, reqAttrName, reqAttrValue, squeak=squeak)

    if element==None:
        metaInfo[field] = ""
        if squeak:
            logging.warn("path %s, no element for metaInfo field '%s', tag %s" % (path, field, etree.tag))
        return

    if convertToAscii==True:
        metaInfo[field] = treeToAsciiText(element)
    elif isAbstract==True:
        metaInfo[field] = pmcAbstractToHtml(element)
    else:
        metaInfo[field] = element.text

    if metaInfo[field] != None:
        metaInfo[field] = metaInfo[field].replace("\n", " ")
        metaInfo[field] = metaInfo[field].replace("\t", " ")
    
    if metaInfo[field] == None:
        metaInfo[field] = ""

def extractNames(contribNodeList):
    """ 
    helper function for parsePmcXml, get the author names from the contrib
    section of a pmc xml file
    """ 
    surnames = []
    firstnames = []
    emails = []
    affiliations = []
    if contribNodeList==None or len(contribNodeList)==0:
        return "", "", ""

    for contrib in contribNodeList:
        contribType = contrib.attrib.get("contrib-type",None)
        if contrib.tag=="aff":
            instEl = contrib.find("institution")
            addrEl = contrib.find("addr-line")
            countryEl = contrib.find("country")
            affParts = []
            if instEl!=None and instEl.text!=None:
                affParts.append(instEl.text)
            if addrEl!=None and addrEl.text!=None:
                affParts.append(addrEl.text)
            if countryEl!=None and countryEl.text!=None:
                affParts.append(countryEl.text)
            if len(affParts)==0:
                # try just the raw text of the aff-tag if anyth else fails
                affText = treeToAsciiText(contrib)
                if affText!="":
                    affParts.append(affText)
            affiliations.append(", ".join(affParts))
                
        if contribType=="author":
            nameEl = contrib.find("name")
            if nameEl==None:
                continue

            surnameEl = nameEl.find("surname")
            if surnameEl!=None:
                surname = surnameEl.text
                if surname==None:
                    surname=""
                surnames.append(surname)

            firstnameEl = nameEl.find("given-names")
            if firstnameEl!=None:
                firstname = firstnameEl.text
                if firstname==None:
                    firstname=""
                firstnames.append(firstname)

            emailEl = contrib.find("email")
            if emailEl!=None and emailEl.text!=None:
                emails.append(emailEl.text)
            emailEl = contrib.find("address/email")
            if emailEl!=None and emailEl.text!=None:
                emails.append(emailEl.text)
    names = []
    for i in range(0, min(len(surnames), len(firstnames))):
        names.append(surnames[i]+", "+firstnames[i])

    nameStr = "; ".join(names)
    emailStr = "; ".join(emails)
    affStr = "; ".join(affiliations)

    return nameStr, emailStr, affStr

def minimumYear(articleMeta):
    """ helper for parsePmcXml: extracts the minimum year of all pubdate entries """
    pubDates = articleMeta.findall("pub-date")
    years = []

    if pubDates!=None:
        for date in pubDates:
            year = date.find("year")
            if year==None:
                continue
            yearString = year.text
            try:
                years.append(int(yearString))
            except:
                logging.debug("exception when casting the year to int")
                pass
        
    if len(years)==0:
        logging.debug("no valid years found")
        return "noValidYear"

    minYear = str(min(years))
    logging.log(5, "determined minimum year as %s" % minYear)
    return minYear

def parseSupplMatEl(suppData):
    " unusual case: one suppl tag, many media tags "
    suppFileInfo = {}
    medias = suppData.findall("media")
    logging.debug("Found %d medias" % len(medias))
    mediaCount = 0
    for mediaEl in medias:
        fname = mediaEl.attrib.get("href","")
        labelText = mediaEl.findtext("label")
        descParts = []
        descParts.append("Supp. File %d" % (mediaCount+1))
        if labelText!=None:
            descParts.append(labelText)
        #descParts.append("filename: "+fname)
        desc = " - ".join(descParts)
        logging.debug("Found suppl file fname=%s, desc=%s" % (fname, desc))
        suppFileInfo[fname]=desc
        mediaCount+=1
    return suppFileInfo

def parseSupplMatList(suppDataList):
    " standard case: a list of supplMaterial tags, each with one media tag "
    suppFileInfo = {}
    suppCount = 0
    for suppData in suppDataList:
        shortDesc = suppData.attrib.get("id","")
        longDesc = suppData.findtext("caption/title")
        logging.debug("Found suppl file id=%s, title=%s" % (shortDesc, longDesc))

        suppDescParts=[]
        if longDesc!=None:
            suppDescParts.append(longDesc)
        if shortDesc==None:
            suppDescParts.append(shortDesc)
            shortDesc=""
        if (longDesc!=None and not longDesc.startswith("Additional")) or \
            longDesc==None:
            suppDescParts.append("Supp. File %d" % suppCount)

        medias = suppData.findall("media")
        for media in medias:
            desc = suppData.findtext("label")
            fname = media.attrib.get("href","")
            #suppDescParts.append("filename: %s" % fname)
            suppDesc = " - ".join(suppDescParts)
            suppFileInfo[fname]=suppDesc
            suppCount += 1
            logging.debug("Found media filename, href=%s" % fname)
    return suppFileInfo

def iterAllCitElements(tree):
    " iterate over all sorts of citation elements "
    for citEl in tree.findall("citation"):
        yield citEl
    for citEl in tree.findall("mixed-citation"):
        yield citEl
    for citEl in tree.findall("citation-element"):
        yield citEl

def parseCit(citEl):
    " construct a pubStore.RefRec from a XML citation-like element and return it "
    """ <ref id="pone.0046865-Qu2">
        <label>44</label>
        <mixed-citation publication-type="journal">
          <name>
            <surname>Qu</surname>
            <given-names>Z</given-names>
          </name>, <name><surname>Fischmeister</surname><given-names>R</given-names></name>, <name><surname>Hartzell</surname><given-names>C<
      </ref>

     <citation citation-type="journal">
          <person-group person-group-type="author">
            <name>
              <surname>Chen</surname>
              <given-names>JK</given-names>
            </name>
    """
    citDict = {}

    authors = []
    nameEls = citEl.findall(".//name")
    for nameEl in nameEls:
        surNameEl = nameEl.find("surname")
        givenNameEl = nameEl.find("given-names")
        if surNameEl!=None and givenNameEl!=None:
            surName = surNameEl.text
            givenName = givenNameEl.text
            if givenName==None:
                givenName=""
            if surName==None:
                surName =""
            authors.append(surName+", "+givenName)
    authorStr = "; ".join(authors)
    citDict["authors"] = authorStr

    metaInfoFromTree(citDict, "journal", citEl, "source", squeak=False)
    metaInfoFromTree(citDict, "year", citEl, "year", squeak=False)
    metaInfoFromTree(citDict, "vol", citEl, "volume", squeak=False)
    metaInfoFromTree(citDict, "issue", citEl, "issue", squeak=False)
    metaInfoFromTree(citDict, "month", citEl, "month", squeak=False)
    metaInfoFromTree(citDict, "page", citEl, "fpage", squeak=False)
    metaInfoFromTree(citDict, "title", citEl, "article-title", squeak=False)
    # <pub-id pub-id-type="pmid">19426717</pub-id></mixed-citation>
    metaInfoFromTree(citDict, "pmid", citEl, "pub-id", reqAttrName="pub-id-type", reqAttrValue="pmid", squeak=False)
    metaInfoFromTree(citDict, "doi", citEl, "pub-id", reqAttrName="pub-id-type", reqAttrValue="doi", squeak=False)

    return pubStore.RefRec(**citDict)

def parseRefSection(articleTree):
    " return references as a list of namedtuples "
    rows = []

    refLists = articleTree.findall(".//ref-list")
    for refListEl in refLists:
        for refEl in refListEl.findall(".//ref"):
            for citEl in iterAllCitElements(refEl):
                citRow = parseCit(citEl)
                if citRow!=None:
                    rows.append(citRow)
    return rows

def parsePmcXml(xml, metaInfo):
    """ return the meta information contained in an pmc xml file as a metaInfo-dictionary """

    logging.debug("Parsing pmc nxml data")
    articleTree = etreeFromXml(xml) # got inserted automatically by html parser

    if articleTree==None or len(articleTree)==0 or articleTree.tag!="article":
        if articleTree.tag!="article":
            logging.info("Not an article: %s tag found" % articleTree.tag)
        else:
            logging.warn("No article tag found, empty file?")

        metaInfo["articleType"]="noContent"
        return xml, metaInfo

    metaInfo["articleType"]=articleTree.attrib["article-type"]
    journalTree = articleTree.find("front/journal-meta")
    metaInfoFromTree(metaInfo, "journal", journalTree, "journal-title", squeak=False)
    if metaInfo["journal"]=="":
        metaInfoFromTree(metaInfo, "journal", journalTree, "journal-title-group/journal-title")
    metaInfoFromTree(metaInfo, "printIssn", journalTree, "issn", reqAttrName="pub-type", \
        reqAttrValue='ppub', squeak=False)
    metaInfoFromTree(metaInfo, "eIssn", journalTree, "issn", reqAttrName="pub-type", \
        reqAttrValue='epub', squeak=False)

    articleMeta = articleTree.find("front/article-meta")
    metaInfoFromTree(metaInfo, "pmcId", articleMeta, "article-id", reqAttrName="pub-id-type", reqAttrValue='pmc')
    metaInfo["externalId"]="PMC"+metaInfo["pmcId"]
    metaInfo["fulltextUrl"]="http://www.ncbi.nlm.nih.gov/pmc/articles/%s" % metaInfo["externalId"]
    metaInfo["source"]="pmcftp"
    if metaInfo["journal"]=="":
        logging.warn("Could not find a journal title for PMC %s" % metaInfo["externalId"])
    metaInfoFromTree(metaInfo, "pmid", articleMeta, "article-id", reqAttrName="pub-id-type", reqAttrValue="pmid", squeak=False)
    metaInfoFromTree(metaInfo, "doi", articleMeta, "article-id", reqAttrName="pub-id-type", reqAttrValue="doi", squeak=False)
    metaInfoFromTree(metaInfo, "title", articleMeta, "title-group/article-title", convertToAscii=True)
    if metaInfo["title"]==None or metaInfo["title"]=="":
        metaInfoFromTree(metaInfo, "title", articleMeta, "title-group/article-title", convertToAscii=True)

    minYear = minimumYear(articleMeta)
    metaInfo["year"]=minYear

    contribs = articleMeta.find("contrib-group")
    #print toXmlString(articleMeta.find("contrib-group"))
    nameStr, emailStr, affStr = extractNames(contribs)
    metaInfo["authors"]=nameStr
    metaInfo["authorEmails"] = emailStr
    metaInfo["authorAffiliations"] = affStr

    metaInfoFromTree(metaInfo, "vol", articleMeta, "volume", squeak=False)
    metaInfoFromTree(metaInfo, "issue", articleMeta, "issue", squeak=False)
    metaInfoFromTree(metaInfo, "page", articleMeta, "fpage", squeak=False)
    metaInfoFromTree(metaInfo, "abstract", articleMeta, "abstract", isAbstract=True, squeak=False)

    # supp data files
    suppCount=0
    suppFileInfo = {}
    # there are two different formats for suppl material:
    # either many <supplementary-material>-tags, each with a media
    # or one supplementary-material tag, with many media tags
    suppDataEls = articleTree.findall(".//supplementary-material")
    logging.debug("Found %d suppl-material tags" % len(suppDataEls))
    if len(suppDataEls)==1:
        suppFileInfo = parseSupplMatEl(suppDataEls[0])
    else:
        suppFileInfo = parseSupplMatList(suppDataEls)

    logging.log(5, "Complete Meta info:"+str(metaInfo))

    metaInfo     = pubGeneric.dictToUnicode(metaInfo)
    #suppFileInfo = suppFileInfo)
    
    refData = parseRefSection(articleTree)

    return metaInfo, suppFileInfo, refData
    
def createIndexFile( inDir, inFname, pmcToPmid, \
        outFname, updateId, chunkSize, chunkCount, startId, \
        donePmcIds, ignoreHeaders=True):
    """ write filenames from inFname to outFname in format (numId, indir, infile, outdir, outfile)
    starting id is numId 
    """
    inAbsFname = os.path.join(inDir, "file_list.txt")
    if not os.path.isfile(inAbsFname):
        logging.info("Could not find %s, check localMirrorDir in pubtools.conf" % inAbsFname)
        sys.exit(1)
    else:
        logging.info("Copying pmc index from %s to %s, assigning identifers and chunk IDs" % (inAbsFname, outFname))

    inList = open(inAbsFname)
    if ignoreHeaders:
        inList.readline()

    # first read in all new pmcIds
    pmcIdsFiles = []
    logging.info("%d pmcIDs have already been processed" % len(donePmcIds))
    skippedPmcIds = 0
    lineCount = 0
    for line in inList:
        lineCount +=1 
        fields = line.strip("\n").split("\t")
        inRelPath = fields[0]
        pmcId = fields[2]
        if pmcId in donePmcIds:
            skippedPmcIds += 1
            continue
        pmcIdsFiles.append((pmcId, inRelPath))

    if len(pmcIdsFiles)==0:
        logging.warn("No new pmcIds to process")
        return None, None
    else:
        logging.critical("%d pmcIDs will be processed now" % len(pmcIdsFiles))


    if chunkSize!=None:
        chunkCount = int(math.ceil(float(len(pmcIdsFiles)) / chunkSize))
        logging.info("Number of chunks is %d (=%d / %d)" % (chunkCount, len(pmcIdsFiles), chunkSize))

    # then output new index file 
    outList = open(outFname, "w")
    headers = ["articleId", "chunkId", "pmcId", "pmid", "srcDir", "filePath"]
    outList.write("#"+"\t".join(headers)+"\n")

    newPmcIds = []
    for idCount, pmcIdRelPath in enumerate(pmcIdsFiles):
        pmcId, inRelPath = pmcIdRelPath
        numId=(startId)+(idCount)
        pmid = pmcToPmid.get(pmcId, "0")
        if pmid==None or pmid==0:
            logging.info("No PMID for article %s" % pmcId)
        chunkDiv = numId % chunkCount
        chunkId = "%d_%.5d" % (updateId, chunkDiv)
        outPath = chunkId+".zip"

        data = [str(numId), chunkId, pmcId, pmid, inDir, inRelPath]
        outList.write("\t".join(data)+"\n")
        newPmcIds.append(pmcId)

    outList.close()
    logging.info("Found %d lines/PMCIDs in index, %d of these had been processed before" % \
        (lineCount, skippedPmcIds))
    return numId, newPmcIds

def parsePmcIds(pmcTabFname):
    " parse PMC-ids.csv.gz and return as dict pmcId (e.g. PMCxxx) => pmid (e.g. 123454) "
    logging.info("parsing %s" % pmcTabFname)
    data = {}
    reader = csv.reader(gzip.open(pmcTabFname, 'rb'))
    Record = None
    for row in reader:
        if Record==None:
            # first line
            headers = [h.replace(" ", "") for h in row]
            Record = collections.namedtuple('pmcRec', headers)
        else:
            # all other lines
            if len(row)!=len(headers):
                # bug in Mar 2013 and Jun 2013
                logging.warn("Line with illegal number of fields in %s" % pmcTabFname)
                continue
            rec = Record(*row)
            pmid = rec.PMID
            if pmid=="":
                pmid = "0"
            data[rec.PMCID]=pmid
    return data

def splitIndexAndSubmitConvertJobs(pmcDirectory, finalOutDir, minId, runner, chunkCount):
    " create and split index file and submit jobs to cluster system, one per index chunk "
    # parse info from last runs
    logging.info("Reading index files, splitting")
    updateId, articleId, donePmcIds = pubStore.parseUpdatesTab(finalOutDir, minArticleId=minId)

    # get correct PMIDs (10% of PMIDs are wrong in XML)
    pmcTabFname = join(pmcDirectory, "PMC-ids.csv.gz")
    pmcToPmid = parsePmcIds(pmcTabFname)

    # create temporary directory and schedule for deletion
    outDir     = tempfile.mktemp(dir = finalOutDir, prefix = "update%d.tmp." % updateId)
    os.makedirs(outDir)
    maxCommon.delOnExit(outDir)

    # write big index
    indexFile = join(outDir, "%d_index.tab" % updateId)
    fileListFname = "file_list.txt" 
    if int(updateId)==0:
        logging.info("This is the first run, not setting chunkSize")
        chunkSize = None
    else:
        logging.debug("This is not the first run, need to guess chunk size")
        chunkSize = pubStore.guessChunkSize(finalOutDir)

    lastArticleId, newPmcIds = createIndexFile(pmcDirectory, fileListFname, pmcToPmid, indexFile, updateId, \
        chunkSize, chunkCount, articleId, donePmcIds)
    if lastArticleId == None:
        return 

    indexDir = join(outDir, "index.tmp.split")
    chunkIds = pubStore.splitTabFileOnChunkId(indexFile, indexDir)

    for chunkId in chunkIds:
        logging.debug("submitting job for chunk %s" % chunkId)
        outFname = os.path.join(outDir, chunkId+".articles.gz")
        chunkFname = os.path.join(indexDir, chunkId)
        maxCommon.mustNotExist(outFname)
        command = "%s %s {check in line %s} {check out exists %s}" % (sys.executable, progFile, chunkFname, outFname)
        runner.submit(command)
    runner.finish(wait=True)

    pubGeneric.concatDelLogs(outDir, finalOutDir, "%d.log" % updateId)
    pubStore.moveFiles(join(outDir, "refs"), join(finalOutDir, "refs"))
    pubStore.moveFiles(outDir, finalOutDir)
    pubStore.appendToUpdatesTxt(finalOutDir, updateId, lastArticleId, newPmcIds)
    shutil.rmtree(outDir)

def extractTgz(baseDir, fname):
    """ extract all files from pmc tarfile, returns dict with 'filename' -> content
    """
    tarfname = os.path.join(baseDir, fname)
    if not os.path.exists(tarfname):
        logging.warn("Could not find file %s, internal error?\n" % tarfname)
        return None

    logging.log(5, "Opening tarfile %s" % tarfname)
    try:
        tarObject = tarfile.open(tarfname)
    except tarfile.ReadError:
        logging.error("Could not open %s, broken tarfile" % tarfname)
        return None

    # is this necessary? why doesn't tarfile always throw an exception?
    if tarObject==None:
        logging.error("No results when opening file %s, data corruption error?" % tarfname)
        return None

    fileData = {}
    logging.debug("Extracting tarfile %s " % (tarfname))
    try:
        members = tarObject.getmembers()
    except IOError:
        logging.error("Could not extract %s" % tarfname)
        return None

    for tarinfo in members:
        if tarinfo.isfile():
            fileObj = tarObject.extractfile(tarinfo)
            fileName = os.path.basename(tarinfo.name)
            fileSize = tarinfo.size
            if fileSize > MAXFILESIZE:
                logging.warn("%s:%s has size %d, bigger than %d, skipping" % \
                    (fname, fileName, fileSize, MAXFILESIZE))
                continue
            fileData[fileName] = fileObj.read()
    logging.debug("Filenames in tgz file: %s" % str(fileData.keys()))
    return fileData

def getNxmlFile(fileDataDict):
    """ pull out nxml data from filename->data dict """
    if fileDataDict==None:
        return None, None

    nxmlData = None
    for fname, data in fileDataDict.iteritems():
        if fname.endswith(".nxml"):
            if nxmlData!=None:
                logging.error("several nxml files found: %s" % (fname))
            nxmlData = data
            nxmlName = fname
    if nxmlData==None:
        logging.error("No Nxml file found, among files: %s" % str(fileDataDict.keys()))
        nxmlData = None
    nxmlName = os.path.basename(nxmlName)
    return nxmlName, nxmlData
            
def createFileData(tgzName, nxmlName, nxmlData, suppFileDict, extractedFiles, pmcId):
    " given file data and meta data, create a list of fileData "
    fileDataList = []
    #urlPrefix = "pmcftp://"+tgzName
    urlPrefix = "http://www.ncbi.nlm.nih.gov/pmc/articles/%s" % pmcId

    # add xml file data
    xmlFile = pubStore.createEmptyFileDict(
        url=urlPrefix,
        externalId = pmcId,
        content=nxmlData,
        fileType="main",
        desc = "main text (xml)",
        mimeType="text/xml",
        locFname = tgzName+":"+nxmlName, 
        )
    fileDataList.append(xmlFile)

    # try to find pdf files and add pdf file data
    pdfName = basename(nxmlName).replace(".nxml", ".pdf")
    if pdfName!=nxmlName and pdfName in extractedFiles:
        pdfFile = pubStore.createEmptyFileDict( 
            url=urlPrefix+"/pdf/",
            externalId = pmcId,
            content=extractedFiles[pdfName],
            desc = "main text (pdf)",
            fileType="main",
            mimeType="application/pdf",
            locFname = tgzName+":"+pdfName
            )
        fileDataList.append(pdfFile)
        
    # add all suppl files
    for suppFname, suppDesc in suppFileDict.iteritems():
        if suppFname not in extractedFiles:
            logging.error("File %s referenced in xml, not found in tgz. skipping." % suppFname)
            continue
        suppData = extractedFiles[suppFname]
        mimeType, dummy = mimetypes.guess_type(suppFname)
        suppFile = pubStore.createEmptyFileDict(
            externalId = pmcId,
            url=urlPrefix+"/bin/"+basename(suppFname),
            content=suppData,
            desc=suppDesc,
            fileType="supp",
            mimeType=mimeType,
            locFname=tgzName+":"+suppFname
            )
        fileDataList.append(suppFile)
    return fileDataList
    
def convertOneChunk(inIndexFile, outFile):
    """ 
    get files from inIndexFile, parse Xml, 
    convert all supplp files to
    ASCII and write everything to outfile 
    """ 
    store = pubStore.PubWriterFile(outFile)

    for row in maxCommon.iterTsvRows(inIndexFile):
        articleId = row.articleId
        baseDir = row.srcDir
        tgzName = row.filePath
        extractedFiles = extractTgz(baseDir, tgzName)
        if extractedFiles == None:
            continue
        nxmlName, nxmlData = getNxmlFile(extractedFiles)
        if nxmlData==None:
            continue
        articleData = pubStore.createEmptyArticleDict(publisher="pmc")
        articleData, suppFileDict, refs = parsePmcXml(nxmlData, articleData)
        articleData["origFile"] = tgzName+":"+nxmlName
        articleData["publisher"] = "pmc"

        fileWritten = False
        fileDataList = createFileData(tgzName, nxmlName, nxmlData, suppFileDict, extractedFiles, \
            articleData["externalId"])
        fileIdx = 0
        for fileData in fileDataList:
            fileId   = (1000*int(articleId))+fileIdx
            fileDebugDesc = fileData["locFname"]
            fileData = pubGeneric.toAsciiEscape(fileData)
            fileIdx+=1

            if fileData==None:
                logging.warn("Could not convert file %s" % fileDebugDesc)
            else:
                store.writeFile(articleId, fileId, fileData, externalId=articleData["externalId"])
                fileWritten = True

        # only write article meta information if at least one article file could be converted
        if fileWritten:
            store.writeArticle(articleId, articleData)
            store.writeRefs(articleData, refs)
        else:
            logging.warn("Not writing anything, not a single article file converted")

    store.close()

# ----------- MAIN --------------
def main(args, options):
    if args==[] and not options.auto:
        parser.print_help()
        exit(1)

    if len(args)==1:
        # debug mode, just specify an xml file
        pubGeneric.setupLogging(progFile, options)
        articleData = pubStore.createEmptyArticleDict()
        articleData, suppFileDict, refData = parsePmcXml(open(args[0]).read(), articleData)
        for key, val in articleData.iteritems():
            print key, val
        print suppFileDict
        print "* references:"
        for row in refData:
            print row
        sys.exit(0)

    if options.auto:
        pmcDir, outDir = join(pubConf.extDir, "pmc"), join(pubConf.textBaseDir, "pmc")
    else:
        pmcDir, outDir = args
    maxCommon.mustExist(pmcDir)

    # first clean old temp directories
    tmpMask = join(outDir, "update*.tmp*")
    cmd = "rm -rf %s" % tmpMask
    maxCommon.runCommand(cmd, ignoreErrors=True)

    minId = options.minId
    chunkCount = options.chunkCount
    dryRun = options.dryRun

    pubGeneric.setupLogging(progFile, options)

    if os.path.isdir(pmcDir):
        maxCommon.mustExist(outDir)
        pubGeneric.setLockFile(outDir, "pubConvPmc")
        maxCommon.mustExistDir(outDir)
        runner = pubGeneric.makeClusterRunner(__file__, maxJob=pubConf.convertMaxJob, headNode=options.cluster)
        if not options.updateDb:
            splitIndexAndSubmitConvertJobs(pmcDir, outDir, minId, runner, chunkCount)
        tsvFnames = glob.glob(join(outDir,"*.articles.gz"))
        dbPath = join(outDir, "articles.db")
        pubStore.loadNewTsvFilesSqlite(dbPath, "articles", tsvFnames)
    else:
        inIndexFile = pmcDir
        outFile = outDir
        logFname = join(dirname(outFile), basename(outFile).split(".")[0]+".log")
        pubGeneric.setupLogging(__file__, options, logFileName=logFname)
        convertOneChunk(inIndexFile, outFile)

main(args, options)

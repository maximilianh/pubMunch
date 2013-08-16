import logging, optparse, os, glob, zipfile, types, re, tempfile, shutil, sys, gzip
from os.path import *
import pubGeneric, maxRun, pubStore, pubConf, maxCommon, pubXml, maxTables, pubCompare

# convert springer zip files to pubtools format
# lots of code pasted from pubConvElsevier

# load lxml parser, with fallback to default python parser
try:
    from lxml import etree # you can install this. Debian/Redhat package: python-lxml, see also: codespeak.net/lxml/installation.html
    import lxml
except ImportError:
    import xml.etree.cElementTree as etree # this is the slower, python2.5 default package

# ==== FUNCTIONs =====
def createIndexFile(inDir, zipFilenames, indexFilename, updateId, minId, chunkSize):
    """ 
    write xml.Meta-filenames in zipfiles in inDir to indexFilename in format
    (numId, chunkId, zipName, fileName), starting id is minId 

    returns the last articleId that was assigned
    """
    logging.info("Writing to %s" % indexFilename)
    indexFile = open(indexFilename, "w")
    headers = ["articleId", "chunkId", "zipFilename", "filename"]
    indexFile.write("\t".join(headers)+"\n")

    logging.debug("Processing these files in %s: %s" % (inDir, zipFilenames))
    if len(zipFilenames)==0:
        logging.info("Nothing to convert, all files are already marked done in updates.tab")
        sys.exit(0)

    numId = minId
    xmlCount = 0
    i = 0
    for fname in zipFilenames:
        zipFilename = join(inDir, fname)
        logging.info("Indexing %s, %d files left" % (zipFilename, len(zipFilenames)-i))
        i+=1
        try:
            zipNames = zipfile.ZipFile(zipFilename).namelist()
        except zipfile.BadZipfile:
            logging.error("Bad zipfile: %s" % zipFilename)
            continue

        zipRelName = basename(zipFilename)
        for fileName in zipNames:
            if not fileName.endswith(".xml.Meta"):
                continue
            xmlCount += 1

            chunkId = ((numId-minId) / chunkSize)
            chunkString = "%d_%05d" % (updateId, chunkId)
            data = [str(numId), chunkString, zipRelName, fileName]
            indexFile.write("\t".join(data)+"\n")
            numId+=1
    indexFile.close()
    logging.info("Processed %d zip files, with %d xml files" % \
        (i, xmlCount))
    return numId

def submitJobs(runner, zipDir, splitDir, idFname, outDir):
    chunkIds = os.listdir(splitDir)
    for chunkId in chunkIds:
        chunkFname = join(splitDir, chunkId)
        outFname = os.path.join(outDir, chunkId+".articles.gz")
        maxCommon.mustNotExist(outFname)
        thisFilePath = __file__
        command = "%s %s %s {check in line %s} {check in line %s} {check out exists+ %s}" % \
            (sys.executable, thisFilePath, zipDir, chunkFname, idFname, outFname)
        runner.submit(command)
    runner.finish(wait=True)

def findText(tree, string):
    el = tree.find(string)
    if el!=None:
        return el.text
    else:
        return ""

def parseXml(tree, data):
    """
    use elementTree to parse Springer A++, fill dict data with results or None if not succesful
    """
    hasFulltext = False

    data["source"]          = "springer"
    journalEl = tree.find("Journal")
    jiEl = journalEl.find("JournalInfo")
    data["printIssn"]       = findText(jiEl, "JournalPrintISSN")
    data["eIssn"]           = findText(jiEl, "JournalElectronicISSN")
    data["journal"]         = findText(jiEl, "JournalTitle")
    subjGroupEl = jiEl.find("JournalSubjectGroup")
    data["articleSection"]  = findText(subjGroupEl, "SubjectCollection")

    volEl = journalEl.find("Volume/VolumeInfo")
    data["vol"]             = findText(volEl, "VolumeIDStart")

    issEl = journalEl.find("Volume/Issue/IssueInfo")
    data["issue"]           = findText(issEl, "IssueIDStart")
    data["year"]            = findText(issEl, "IssueHistory/OnlineDate/Year")
    if data["year"]=="":
        data["year"]        = findText(issEl, "IssueHistory/PrintDate/Year")
    if data["year"]=="":
        data["year"]        = findText(issEl, "IssueHistory/CoverDate/Year")

    artEl = journalEl.find("Volume/Issue/Article/ArticleInfo")
    doi = findText(artEl, "ArticleDOI")
    data["doi"]             = doi
    data["title"]           = findText(artEl, "ArticleTitle")
    data["articleType"]     = findText(artEl, "ArticleCategory")
    data["page"]            = findText(artEl, "ArticleFirstPage")

    springerBaseUrl = "http://link.springer.com/article/"
    data["fulltextUrl"]     = springerBaseUrl+doi

    headEl = journalEl.find("Volume/Issue/Article/ArticleHeader")
    if headEl==None:
        logging.error("No ArticleHeader element")
        return None

    auGroupEl = headEl.find("AuthorGroup")
    names = []
    emails = []
    for authEl in auGroupEl.findall("Author"):
        givenNames = []
        for givenEl in authEl.findall("AuthorName/GivenName"):
            givenNames.append(givenEl.text)
        givenName = " ".join(givenNames)
        famName = findText(authEl, "AuthorName/FamilyName")
        name = famName+", "+givenName
        names.append(name)

        emailEl = authEl.find("Contact/Email")
        if emailEl!=None:
            emails.append(emailEl.text)
    data["authors"] = "; ".join(names)
    data["authorEmails"] = "; ".join(emails)

    abEl = headEl.find("Abstract")
    if abEl==None:
        #logging.error("No abstract?")
        #return None
        data["abstract"] = ""
    else:
        abParts = []
        headCount = 0
        for childEl in abEl.iter():
            if childEl.tag=="Heading":
                # skip first header, is always "Abstract"
                if headCount==0:
                    headCount += 1
                    continue
                else:
                    abParts.append("<b>"+childEl.text+": <b>")
            if childEl.tag=="Para":
                abParts.append(childEl.text+"<p>")
        data["abstract"] = "".join(abParts).rstrip("<p>")

    kwGroupEl = headEl.find("KeywordGroup")
    kwList = []
    if kwGroupEl!=None:
        for kwEl in kwGroupEl.findall("Keyword"):
            kwList.append(kwEl.text)
    data["keywords"] = "; ".join(kwList)

    data["publisher"] = "springer"
    data["externalId"] = data["doi"]
    return data

def createFileData(articleData, mimeType, asciiString):
    fileData = pubStore.createEmptyFileDict()
    fileData["desc"] = ""
    fileData["url"] = articleData["fulltextUrl"].replace("/article/", "/content/pdf/")+".pdf"
    fileData["content"] = asciiString
    fileData["mimeType"] = mimeType
    fileData["fileType"] = "main"
    return fileData

def parseDoi2Pmid(baseDir):
    " parse doi2pmid.tab.gz and return as dict "
    fname = join(baseDir, "doi2pmid.tab.gz")
    if not isfile(fname):
        logging.info("Could not find %s, not adding external PMIDs" % fname)
        return {}
    else:
        logging.info("Found %s, reading external PMIDs" % fname)
    lines = gzip.open(fname)
    data = {}
    for line in lines:
        line = line.strip()
        fields = line.split("\t")
        if len(fields)!=2:
            logging.error("Could not parse line %s" % line)
            continue
        doi, pmid = fields
        pmid = int(pmid)
        data[doi]=pmid
    return data

def parseDoneIds(fname):
    " parse all already converted identifiers from inDir "
    print fname
    doneIds = set()
    if os.path.getsize(fname)==0:
        return doneIds

    for row in maxCommon.iterTsvRows(fname):
        doneIds.add(row.doi)
    logging.info("Found %d identifiers of already parsed files" % len(doneIds))
    return doneIds
            
def convertOneChunk(zipDir, inIndexFile, inIdFile, outFile):
    """ 
    get files from inIndexFile, parse Xml, 
    write everything to outfile in ascii format
    """ 
    store = pubStore.PubWriterFile(outFile)

    # read all already done IDs
    doneIds = parseDoneIds(inIdFile)

    # open output id files
    idFname = join(dirname(outFile), basename(outFile).split(".")[0]+".ids.tab")
    logging.debug("Writing ids to %s" % idFname)
    idFh = open(idFname, "w")
    idFh.write("#articleId\tdoi\tpmid\n")

    pmidFinder = pubCompare.PmidFinder()

    i = 0
    inRows = list(maxCommon.iterTsvRows(inIndexFile))
    logging.info("Converting %d files" % len(inRows))
    convCount = 0
    pdfNotFound = 0
    for row in inRows:
        # read line
        i+=1
        articleId = row.articleId
        zipFilename, filename = row.zipFilename, row.filename
        
        # construct pdf filename from xml filename
        xmlDir = dirname(filename)
        xmlBase = basename(filename).split(".")[0]
        pdfFname = join(xmlDir, "BodyRef", "PDF", xmlBase+".pdf")

        articleId=int(articleId)

        # open xml and pdf files from zipfile
        fullZipPath = join(zipDir, zipFilename)
        zipFile = zipfile.ZipFile(fullZipPath)
        logging.debug("Parsing %s, file %s, %d files left" % (fullZipPath, filename, len(inRows)-i))
        xmlString = zipFile.open(filename).read()
        try:
            pdfString = zipFile.open(pdfFname).read()
        except KeyError:
            logging.error("Could not find pdf file %s, skipping article" % pdfFname)
            pdfNotFound += 1
            continue

        # parse xml
        try:
            xmlTree   = pubXml.etreeFromXml(xmlString)
        except lxml.etree.XMLSyntaxError:
            logging.error("XML parse error, skipping file %s, %s" % (zipFilename, filename))
            continue

        articleData = pubStore.createEmptyArticleDict(publisher="springer")
        articleData = parseXml(xmlTree, articleData)

        if articleData==None:
            logging.warn("Parser got no data for %s" % filename)
            continue
        if articleData["doi"] in doneIds:
            logging.error("article %s has already been converted, skipping" % articleData["doi"])
            continue

        articleData["pmid"] = pmidFinder.lookupPmid(articleData)
        articleData["origFile"]=zipFilename+"/"+filename
        articleData["externalId"]=articleData["doi"]

        # convert pdf to ascii
        fileData = createFileData(articleData, "application/pdf", pdfString)
        pubGeneric.toAscii(fileData, "application/pdf")

        # write to output
        store.writeArticle(articleId, articleData)
        store.writeFile(articleId, (1000*(articleId))+1, fileData, externalId=articleData["externalId"])

        # write IDs to separate file 
        idRow = [str(articleData["articleId"]), articleData["doi"], str(articleData["pmid"])]
        idFh.write("\t".join(idRow))
        idFh.write("\n")

        doneIds.add(articleData["doi"])

        convCount += 1
    logging.info("Converted %d files, pdfNotFound=%d" % (convCount, pdfNotFound))
    store.close()
    idFh.close()

def concatDois(inDir, outDir, outFname):
    " concat all dois of id files in inDir to outFname "
    outPath = join(outDir, outFname)
    inMask = join(inDir, "*ids.tab")
    idFnames = glob.glob(inMask)
    logging.debug("Concatting DOIs from %s to %s" % (inMask, outPath))
    dois = []
    for inFname in idFnames:
        for row in maxCommon.iterTsvRows(inFname):
            dois.append(row.doi)

    ofh = open(outPath, "w")
    ofh.write("#doi\n")
    for doi in dois:
        ofh.write("%s\n" % doi)
    ofh.close()

    return outPath
    
def createChunksSubmitJobs(zipDir, outDir, minId, runner, chunkSize):
    """ submit jobs to convert ZIP files from zipDir to outDir
        split files into chunks and submit chunks to cluster system
        write first to temporary dir, and copy over at end of all jobs
        This is based on pubConvElsevier.py
    """
    maxCommon.mustExistDir(outDir)

    updateId, minId, alreadyDoneFiles = pubStore.parseUpdatesTab(outDir, minId)
    #if chunkSize==None:
        #chunkSize  = pubStore.guessChunkSize(outDir)
    assert(chunkSize!=None)

    finalOutDir= outDir
    outDir     = tempfile.mktemp(dir = outDir, prefix = "springerUpdate.tmp.")
    os.mkdir(outDir)

    inFiles = os.listdir(zipDir)
    inFiles = [x for x in inFiles if x.endswith(".zip")]
    # keep order of input of input files for first run
    if len(alreadyDoneFiles)!=0:
        processFiles = set(inFiles).difference(alreadyDoneFiles)
    else:
        processFiles = inFiles

    if len(processFiles)==0:
        logging.info("All updates done, not converting anything")
        return None

    indexFilename = join(outDir, "%d_index.tab" % updateId)
    maxArticleId  = createIndexFile(zipDir, processFiles, indexFilename, updateId, minId, chunkSize)

    indexSplitDir = join(outDir, "indexFiles")
    pubStore.splitTabFileOnChunkId(indexFilename, indexSplitDir)

    idFname = concatDois(finalOutDir, outDir, "doneArticles.tab")
    submitJobs(runner, zipDir, indexSplitDir, idFname, outDir)

    pubGeneric.concatDelIdFiles(outDir, finalOutDir, "%d_ids.tab" % updateId)
    pubGeneric.concatDelLogs(outDir, finalOutDir, "%d.log" % updateId)

    # cleanup, move over, remove whole temp dir
    if isdir(indexSplitDir): # necessary? how could it not be there? 
        logging.info("Deleting directory %s" % indexSplitDir)
        shutil.rmtree(indexSplitDir) # got sometimes exception here...
    pubStore.moveFiles(outDir, finalOutDir)
    shutil.rmtree(outDir)

    pubStore.appendToUpdatesTxt(finalOutDir, updateId, maxArticleId, processFiles)

# this is a job script, so it is calling itself via parasol/bsub/qsub
if __name__=="__main__":
    parser = optparse.OptionParser("""usage: %prog [options] <inIndexFile> <inIdFile> <outFile> - job script to convert a springer fulltext file (given using an index file) from a++ format to ascii""")
    parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages")
    (options, args) = parser.parse_args()
    if args==[]:
        parser.print_help()
        exit(1)

    zipDir, inIndexFile, inIdFile, outFile = args

    # keep log messages in base(outFile).log
    logFname = join(dirname(outFile), basename(outFile).split(".")[0]+".log")
    pubGeneric.setupLogging(__file__, options, logFileName=logFname)

    convertOneChunk(zipDir, inIndexFile, inIdFile, outFile)

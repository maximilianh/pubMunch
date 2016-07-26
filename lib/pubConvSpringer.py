import logging, optparse, os, glob, zipfile, types, re, tempfile, shutil, sys, gzip
from os.path import *
import pubGeneric, maxRun, pubStore, pubConf, maxCommon, pubXml, maxTables, pubCompare

# convert springer zip files to pubtools format
# lots of code pasted from pubConvElsevier
# For documentation on the A++ format, see http://www.springeropen.com/get-published/indexing-archiving-and-access-to-data/xml-dtd

# load lxml parser, with fallback to default python parser
try:
    from lxml import etree # you can install this. Debian/Redhat package: python-lxml, see also: codespeak.net/lxml/installation.html
    import lxml
except ImportError:
    import xml.etree.cElementTree as etree # this is the slower, python2.5 default package

# ==== FUNCTIONs =====
def createIndexFile(inDir, inFnames, indexFilename, updateId, minId, chunkSize):
    """ 
    write xml.Meta-filenames in inFnames in inDir to indexFilename in format
    (numId, chunkId, zipName, fileName), starting id is minId 

    returns the last articleId that was assigned
    """
    logging.info("Writing to %s" % indexFilename)
    indexFile = open(indexFilename, "w")
    headers = ["articleId", "chunkId", "zipFilename", "filename"]
    indexFile.write("\t".join(headers)+"\n")

    #logging.debug("Processing these files in %s: %s" % (inDir, inFnames))
    if len(inFnames)==0:
        logging.info("Nothing to convert, all files are already marked done in updates.tab")
        sys.exit(0)

    numId = minId
    xmlCount = 0
    i = 0
    plainXmlCount = 0
    pm = maxCommon.ProgressMeter(len(inFnames))

    for fname in inFnames:
        inPath = join(inDir, fname)
        i+=1
        chunkId = ((numId-minId) / chunkSize)
        chunkString = "%d_%05d" % (updateId, chunkId)

        if inPath.lower().endswith(".zip"):
            logging.debug("Indexing %s" % (inPath))
            zipFilename = inPath
            # get all relevant names from zipfile
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
                data = [str(numId), chunkString, zipRelName, fileName]
                data = [d.encode("utf8") for d in data]
                indexFile.write("\t".join(data)+"\n")
                numId+=1
        else:
            # just append the filename to the index file
            assert(fname.lower().endswith(".meta"))
            data = [str(numId), chunkString, "", fname]
            indexFile.write("\t".join(data)+"\n")
            numId+=1
            plainXmlCount += 1
        pm.taskCompleted()


    indexFile.close()
    logging.info("Processed %d zip files, with %d xml files in them, and %d plain xml files" % \
        (i, xmlCount, plainXmlCount))
    return numId

def submitJobs(runner, zipDir, splitDir, idFname, outDir):
    chunkIds = os.listdir(splitDir)
    for chunkId in chunkIds:
        chunkFname = join(splitDir, chunkId)
        outFname = os.path.join(outDir, chunkId+".articles.gz")
        maxCommon.mustNotExist(outFname)
        thisFilePath = __file__
        command = "%s %s %s {check in exists %s} %s {check out exists+ %s}" % \
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
    logging.debug("Parsing Springer fields from tree")
    hasFulltext = False

    data["source"]          = "springer"
    journalEl = tree.find("Journal")
    jiEl = journalEl.find("JournalInfo")
    data["printIssn"]       = findText(jiEl, "JournalPrintISSN")
    data["eIssn"]           = findText(jiEl, "JournalElectronicISSN")
    data["journal"]         = findText(jiEl, "JournalTitle")
    subjGroupEl = jiEl.find("JournalSubjectGroup")

    keywords = []
    if subjGroupEl!=None:
        for kwEl in subjGroupEl.findall("JournalSubject"):
            keywords.append(kwEl.text)

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
    titleEl = artEl.find("ArticleTitle")
    if titleEl!=None:
        data["title"]           = pubXml.treeToAsciiText(titleEl)
    else:
        data["title"] = ""

    data["articleType"]     = findText(artEl, "ArticleCategory")
    if data["articleType"]==None:
        data["articleType"] = "unknown"

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
        givenNames = [x for x in givenNames if x!=None]
        givenName = " ".join(givenNames)
        famName = findText(authEl, "AuthorName/FamilyName")
        if famName==None:
            famName = ""
        name = famName+", "+givenName
        names.append(name)

        emailEl = authEl.find("Contact/Email")
        if emailEl!=None:
            emails.append(emailEl.text)
    data["authors"] = "; ".join(names)

    emails = [e for e in emails if e!=None]
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
                    # we are now in some sort of named section
                    if childEl.text!=None:
                        abParts.append("<b>"+childEl.text+": <b>")
            elif childEl.tag=="Para":
                abParts.append(pubXml.treeToAsciiText(childEl))
                #abParts.append(childEl.text+"<p>")
        data["abstract"] = "".join(abParts).rstrip("<p>")

    kwGroupEl = headEl.find("KeywordGroup")
    #keywords = []
    if kwGroupEl!=None:
        for kwEl in kwGroupEl.findall("Keyword"):
            keywords.append(kwEl.text)
    keywords = [k.replace(";", ",") for k in keywords if k!=None]
    data["keywords"] = "; ".join(keywords)

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

def parseDoneIds(fname):
    " parse all already converted identifiers from inDir "
    doneIds = set()
    if os.path.getsize(fname)==0:
        return doneIds

    for row in maxCommon.iterTsvRows(fname):
        doneIds.add(row.doi)
    logging.info("Found %d identifiers of already parsed files" % len(doneIds))
    return doneIds
            
def getDiskData(diskDir, filename):
    " return tuple (xmlString, pdfString) with contents of files, pull them out of disk dir "
    xmlFname = abspath(join(diskDir, filename))
    try:
        xmlString = open(xmlFname).read()
    except IOError:
        logging.error("Could not open XML file %s, this should not happen" % xmlFname)
        return None, None

    xmlDir = dirname(xmlFname)
    xmlBase = basename(xmlFname).replace(".xml.Meta","")
    pdfFname = join(xmlDir, "BodyRef", "PDF", xmlBase+".pdf")
    
    if not isfile(pdfFname):
        logging.error("Could not find pdf file %s, skipping article" % pdfFname)
        return None, None
    pdfString = open(pdfFname).read()

    #print type(xmlFname), type(pdfFname)
    logging.debug(('Returning contents of XML ' + xmlFname))
    logging.debug(('Returning contents of PDF %s' % pdfFname))
    #logging.debug((u'Returning contents of %s and %s' % (xmlFname, pdfFname)).decode("latin1", errors="ignore"))
    return xmlString, pdfString

#lastZipFname = None
#zipFile = None

def zipExtract(tmpDir, zipName, filename):
    """ extract filename in zipName to tmpDir, delete tmpfile and return as string 
    thought that this was faster than python's zipfile, but it isn't
    """
    cmd = ["unzip", "-d", tmpDir, zipName, filename]
    ret = maxCommon.runCommand(cmd, ignoreErrors=True)
    if ret!=0:
        return None
    tmpFname = join(tmpDir, filename)
    data = open(tmpFname).read()
    os.remove(tmpFname)
    return data

def getUpdateData(tmpDir, zipDir, zipFilename, filename):
    " return tuple (xmlString, pdfString) with contents of files, pull them out of update zips "
    # construct pdf filename from xml filename
    xmlDir = dirname(filename)
    xmlBase = basename(filename).split(".")[0]
    pdfFname = join(xmlDir, "BodyRef", "PDF", xmlBase+".pdf")

    # open xml and pdf files from zipfile
    global lastZipFname
    global zipFile
    zipPath = join(zipDir, zipFilename)
    #if lastZipFname!=zipPath:
        #logging.debug("Opening %s" % zipPath)
        #zipFile = zipfile.ZipFile(zipPath)
    logging.debug("Extracting %s, file %s" % (zipPath, filename))
    #xmlString = zipFile.read(filename)
    xmlString = zipExtract(tmpDir, zipPath, filename)
    pdfString = zipExtract(tmpDir, zipPath, pdfFname)
    #try:
        #logging.debug("Extracting %s, file %s" % (zipPath, pdfFname))
        #pdfString = zipFile.read(pdfFname)
    #except KeyError:
    if pdfString==None:
        logging.error("Could not find pdf file %s, skipping article" % pdfFname)
        return None, None
    return xmlString, pdfString

def convertOneChunk(zipDir, inIndexFile, inIdFile, outFile):
    """ 
    get files from inIndexFile, parse Xml, 
    write everything to outfile in ascii format
    """ 
    diskDir = abspath(join(zipDir, "..", "disk"))

    store = pubStore.PubWriterFile(outFile)

    # read all already done IDs
    doneIds = parseDoneIds(inIdFile)

    # open output id files
    idFname = join(dirname(outFile), basename(outFile).split(".")[0]+".ids.tab")
    logging.debug("Writing ids to %s" % idFname)
    idFh = open(idFname, "w")
    idFh.write("#articleId\tdoi\tpmid\n")

    pmidFinder = pubCompare.PmidFinder()

    unzipTmp = pubGeneric.makeTempDir(prefix="pubConvSpringerUnzip", tmpDir=pubConf.getFastTempDir())
    maxCommon.delOnExit(unzipTmp)

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

        if '\xbf' in filename:
            logging.info("Found weird character, skipping file")
            continue
        
        articleData = pubStore.createEmptyArticleDict(publisher="springer")
        if zipFilename=="":
            xmlString, pdfString = getDiskData(diskDir, filename)
            articleData["origFile"] = filename
        else:
            xmlString, pdfString = getUpdateData(unzipTmp, zipDir, zipFilename, filename)
            articleData["origFile"] = zipFilename+":"+filename

        if pdfString==None:
            pdfNotFound+=1
            logging.error("Could not open pdf or xml file")
            continue

        articleId=int(articleId)

        # parse xml
        logging.debug("Parsing XML")
        try:
            xmlTree   = pubXml.etreeFromXml(xmlString)
        except lxml.etree.XMLSyntaxError:
            logging.error("XML parse error, skipping file %s, %s" % (zipFilename, filename))
            continue

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
        logging.debug("converting pdf to ascii")
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
    
def parseDiskFnames(diskDir):
    " parse fileList.txt in diskDir "
    listFname = join(diskDir, "fileList.txt")
    if not isfile(listFname):
        raise Exception("Could not find %s. Please run 'find | grep Meta$ > fileList.txt' in %s." \
            % (listFname, diskDir))

    logging.info("Parsing %s" % listFname)
    fnames = []
    for line in open(listFname):
        line = line.rstrip("\n")
        if line.endswith(".Meta"):
            fnames.append(line)
    logging.info("Found %d XML files in disk directory" % len(fnames))
    return fnames
            

    
def finishUp(finalOutDir):
    " do the final post-batch processing "
    buildDir = pubGeneric.makeBuildDir(finalOutDir, mustExist=True)

    minId = pubConf.identifierStart["springer"]

    pubGeneric.concatDelIdFiles(buildDir, finalOutDir, "%d_ids.tab" % updateId)
    pubGeneric.concatDelLogs(buildDir, finalOutDir, "%d.log" % updateId)

    # cleanup, move over, remove whole temp dir
    if isdir(indexSplitDir): # necessary? how could it not be there? 
        logging.info("Deleting directory %s" % indexSplitDir)
        shutil.rmtree(indexSplitDir) # got sometimes exception here...
    pubStore.moveFiles(buildDir, finalOutDir)
    shutil.rmtree(buildDir)

    pubStore.appendToUpdatesTxt(finalOutDir, updateId, maxArticleId, processFiles)
    pubStore.updateSqlite(finalOutDir)

def createChunksSubmitJobs(inDir, finalOutDir, runner, chunkSize):
    """ submit jobs to convert zip and disk files from inDir to outDir
        split files into chunks and submit chunks to cluster system
        write first to temporary dir, and copy over at end of all jobs
        This is based on pubConvElsevier.py
    """
    maxCommon.mustExistDir(finalOutDir)
    minId = pubConf.identifierStart["springer"]

    buildDir = pubGeneric.makeBuildDir(finalOutDir)

    updateId, minId, alreadyDoneFiles = pubStore.parseUpdatesTab(finalOutDir, minId)
    assert(chunkSize!=None)

    # getting filenames from the disk
    diskDir = join(inDir, "disk")
    if int(updateId)==0 and isdir(diskDir):
        inDiskFiles = parseDiskFnames(diskDir)
    else:
        logging.info("Not first update or no directory %s, not parsing files from springer disk" % diskDir)

    # getting filenames from the updates
    zipDir = join(inDir, "updates")
    inZipFiles = os.listdir(zipDir)
    inZipFiles = [x for x in inZipFiles if x.endswith(".zip")]
    logging.info("Found %d update zip files" % len(inZipFiles))
    # keep order of input files for first run

    if len(alreadyDoneFiles)==0:
        processFiles = inDiskFiles+inZipFiles
    else:
        processFiles = set(inZipFiles).difference(alreadyDoneFiles)

    if len(processFiles)==0:
        logging.info("All updates done, not converting anything")
        os.rmdir(buildDir)
        return None
    else:
        logging.info("Total number of files to convert: %d" % (len(processFiles)))

    indexFilename = join(buildDir, "%d_index.tab" % updateId)
    maxArticleId  = createIndexFile(zipDir, processFiles, indexFilename, updateId, minId, chunkSize)

    indexSplitDir = join(buildDir, "indexFiles")
    pubStore.splitTabFileOnChunkId(indexFilename, indexSplitDir)

    idFname = concatDois(finalOutDir, buildDir, "doneArticles.tab")
    submitJobs(runner, zipDir, indexSplitDir, idFname, buildDir)

    finishUp(buildDir, finalOutDir)

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

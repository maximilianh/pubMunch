import logging, optparse, os, glob, zipfile, types, re, tempfile, shutil, sys, gzip
from os.path import *
import pubGeneric, maxRun, pubStore, pubConf, maxCommon, pubXml

# load lxml parser, with fallback to default python parser
try:
    from lxml import etree # you can install this. Debian/Redhat package: python-lxml, see also: codespeak.net/lxml/installation.html
    import lxml
except ImportError:
    import xml.etree.cElementTree as etree # this is the slower, python2.5 default package

# === CONSTANTS ===================================
# the types of elsevier articles to parse, 
# for a reference, see tag-by-tag 5.0
# http://www.elsevier.com/framework_authors/DTDs/ja50_tagbytag5.pdf
# format:
# (article-type, True if this type is parsed / False to ignore it 
# WE IGNORE: indexes, glossaries and bibliographies!
ELSEVIER_ARTICLE_TAGS = [
        ("converted-article", True),
        ("article", True),
        ("simple-article", True),
        ("book-review", True),
        ("exam",  True),
        ("book", True),
        ("chapter", True),
        ("simple-chapter", True), # ??? not in tag by tag
        ("index", False),
        ("glossary", False),
        ("ehs-book", True),
        ("introduction", True),
        ("examination", True),
        ("fb-non-chapter", True), # front and back matter
        ("bibliography", False),
        ("glossary", True),
        ]

# ==== FUNCTIONs =====
def createIndexFile(inDir, zipFilenames, indexFilename, updateId, minId, chunkCount, chunkSize):
    """ 
    write filenames in zipfiles in inDir to indexFilename in format
    (numId, inDir, zipName, fileName), starting id is minId 

    if chunkCount is None, chunkSize will be used. The idea is that chunkCount tells
    how many pieces the files to split in and chunkSize tells how big one piece
    shall be. Only one of them is needed.

    returns the last articleId that was assigned
    """
    logging.info("Writing to %s" % indexFilename)
    indexFile = open(indexFilename, "w")
    headers = ["articleId", "chunkId", "baseDir", "zipFilename", "filename"]
    indexFile.write("\t".join(headers)+"\n")

    logging.debug("Processing these files in %s: %s" % (inDir, zipFilenames))
    if len(zipFilenames)==0:
        logging.info("Nothing to convert, all files are already marked done in updates.tab")
        sys.exit(0)

    numId = minId
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
            if chunkCount==None:
                chunkId = ((numId-minId) / chunkSize)
            else:
                chunkId = ((numId-minId) % chunkCount)
            chunkString = "%d_%05d" % (updateId, chunkId)
            data = [str(numId), chunkString, inDir, zipRelName, fileName]
            indexFile.write("\t".join(data)+"\n")
            numId+=1
    indexFile.close()
    return numId

#def splitIndex(inDir, outDir, indexDir, minId, chunkCount, chunkSize, indexFilename):
    #""" read inDir, create an index from it and split it chunkCount pieces into outDir.
        #then submit jobs, one for each index file
    #"""
    #lastArticleId = createIndexFile(inDir, indexFilename, minId, chunkCount, chunkSize)
    #chunkIds = pubStore.splitTabFileOnChunkId(indexFilename, indexDir)
    #lastChunkId = max([int(x) for x in chunkIds])
    #return lastChunkId, lastArticleId

def submitJobs(splitDir, outDir, maxJob):
    runner = maxRun.Runner(delayTime=3, maxJob=maxJob)
    chunkIds = os.listdir(splitDir)
    for chunkId in chunkIds:
        chunkFname = join(splitDir, chunkId)
        outFname = os.path.join(outDir, chunkId+".articles.gz")
        maxCommon.mustNotExist(outFname)
        thisFilePath = __file__
        command = "%s %s {check in line %s} {check out exists+ %s}" % (sys.executable, thisFilePath, chunkFname, outFname)
        runner.submit(command)
    runner.finish(wait=True)

def findMainArticleTag(tree):
    # search for main article tag
    for articleTag, parseArticle in ELSEVIER_ARTICLE_TAGS:
        articleEl = tree.find(articleTag)
        if articleEl is not None:
            if parseArticle:
                return articleEl, articleTag
            else:
                return None, None

    logging.error("Unknown article tag")
    sys.exit(1)
    return None

elsNewlineTags = set(["section-title", "simple-para", "para", "label"])

def treeToAscii_Elsevier(tree):
    """ try to convert an elsevier XML file to normal ascii text """
    logging.debug("Converting elsevier tree to ascii text")
    asciiText = ""
    dp = tree.find("document-properties")
    if dp!=None:
        rawTextEl = dp.find("raw-text")
        if rawTextEl!=None:
            rawText = rawTextEl.text
            if rawText!=None:
                try:
                    asciiText = rawText.encode('latin1').decode('utf8')
                except UnicodeEncodeError:
                    asciiText = pubGeneric.forceToUnicode(rawText)
                except UnicodeDecodeError:
                    asciiText = pubGeneric.forceToUnicode(rawText)
                #logging.debug("ascii is %s" % repr(rawText))
                return asciiText, "text/plain"

    articleEl, articleType = findMainArticleTag(tree)
    if articleEl is None:
        return None, None

    asciiText = pubXml.treeToAsciiText(articleEl, addNewlineTags=elsNewlineTags)
    return asciiText, "text/xml"

def sanitizeYear(yearStr):
    """ make sure that the year is really a number:
    split on space, take last element, remove all non-digits"""
    nonNumber = re.compile("\D")
    lastWord = yearStr.split(" ")[-1]
    yearStrClean = nonNumber.sub("", lastWord)
    try:
        year = int(yearStrClean)
    except:
        logging.debug("%s does not look like a year, cleaned string is %s" % (yearStr, yearStrClean))
        year = yearStrClean

    return str(year)

def findYear(line):
    " go over all words and search for likely year "
    for word in line.split():
        if word.isdigit():
            num = int(word)
            if num > 1950 and num < 2020:
                return str(num)
    return ""

def parseElsevier(tree, data):
    """
    use elementTree to parse Elsevier Consys XML to the metaData dictionary
    """
    def findText(tree, string):
        el = tree.find(string)
        if el!=None:
            return el.text
        else:
            return ""

    hasFulltext = False

    # PARSE RDF META INFORMATION
    desc =  tree.find("RDF/Description")
    if desc==None:
        logging.warn("Uppercase RDF/Description not found.")
        desc =  tree.find("rdf/description")

    data["source"]          = "elsevier"
    data["title"]           = findText(desc, "title")
    data["articleType"]     = findText(desc, "aggregationType")
    if data["articleType"]=="":
        logging.error("no article type found")

    data["doi"]             = findText(desc, "doi")
    #data["externalId"]      = data["doi"]
    data["journal"]         = findText(desc, "publicationName")
    data["fulltextUrl"]     = findText(desc, "url")
    data["printIssn"]       = findText(desc, "issn")
    data["page"]            = findText(desc, "startingPage")
    data["issue"]           = findText(desc, "number")
    data["year"]            = sanitizeYear(findText(desc, "coverDisplayDate"))
    if data["year"]=="":
        data["year"] = findYear(findText(desc, "copyright"))
    data["vol"]             = findText(desc, "volume")

    # authors: first try to get from description
    creator = desc.find("creator")
    authors = []
    if creator is not None:
        cSeq = creator.find("seq")
        if cSeq==None:
            cSeq = creator.find("Seq")
        if cSeq is not None:
            for liEl in cSeq.iterfind("li"):
                if liEl.text!=None:
                    authors.append(liEl.text)
    data["authors"]="; ".join(authors)

    # try to get year, title and raw text from "document properties"
    dp = tree.find("document-properties")
    if data["year"]!=None and data["year"]!="":
        dpYear = dp.find("year-first")
        if dpYear is not None:
            data["year"]            = dpYear.text


    if dp.find("raw-text") is not None:
        hasFulltext=True

    # search for main article tag
    articleEl, artType = findMainArticleTag(tree)
    if articleEl==None:
        logging.warn("No article type at all")
        return None
    else:
        data["articleType"]     = artType

    itemEl = articleEl.find("item-info")
    if itemEl!=None:
        #ppiEl = itemEl.find("ppi")
        #data["ppi"] = ppiEl.text
        if data["year"]=="":
            copyEl = itemEl.find("copyright")
            if copyEl!=None:
                data["year"] = copyEl.attrib.get("year", None)

    data["year"] = sanitizeYear(data["year"])

    if data["year"]=="":
        logging.warn("no year for article")



    # PARSE ARTICLE
    if articleEl is None:
        logging.warn("skipping because of article type")
        return None

    # check if a <head> element is present, overwrite previous info with these
    headEl = articleEl.find("head")
    if headEl==None:
        headEl = articleEl.find("simple-head")
    if headEl==None:
        headEl = articleEl

    if headEl==None:
        logging.warn("No head element: article might have no abstract")
    else:
        # overwrite author infos
        authorGroups = headEl.iterfind("author-group")
        authorNames = []
        if authorGroups is not None:
            for authorGroup in authorGroups:
                for authorEl in authorGroup.iterfind("author"):
                    firstName = authorEl.findtext("given-name")
                    if firstName is None:
                        firstName=""
                    famName   = authorEl.findtext("surname")
                    if famName is None:
                        famName = ""
                    authorNames.append(famName+", "+firstName)
            if len(authorNames)!=0:
                data["authors"]="; ".join(authorNames)
            # leave this commented out: the head contains special characters
            # the RDF has translated these already to ASCII
            #data["title"] = findText(headEl, "title") # 

            abstractElList = headEl.findall("abstract")
            abstractString = ""
            for abstractEl in abstractElList:
                if abstractEl.get("class","")=="graphical":
                    continue
                if abstractEl is not None:
                    abstractString = etree.tostring(abstractEl, method="text", encoding="utf8")
                    break
            abstractString = abstractString.replace("\t","").replace("\n", " ")
            abstractString = re.sub("^[ ]*Abstract[ ]+", "", abstractString)
            abstractString = re.sub("^[ ]*Summary[ ]+", "", abstractString)
            data["abstract"] = abstractString

        # only extract XML if there is a body tag
        #bodyEl = articleEl.find("body")
        #if bodyEl is not None:
            #hasFulltext=True

    cleanMetaDict = {}

    # XX do we need this? 
    for key, val in data.iteritems():
        if val==None:
            val="NotFound"
        elif type(val) is not types.UnicodeType:
            val = val.decode("utf8")
        val = val.replace("\t", " ")
        val = val.replace("\n", " ")
        cleanMetaDict[key] = val

    #if not hasFulltext:
        #return None
    #else:
    return cleanMetaDict

def createFileData(articleData, mimeType, asciiString):
    fileData = pubStore.createEmptyFileDict()
    fileData["desc"] = ""
    fileData["url"] = articleData["fulltextUrl"]
    fileData["content"] = asciiString
    fileData["mimeType"] = mimeType
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

def convertOneChunk(inIndexFile, outFile):
    """ 
    get files from inIndexFile, parse Xml, 
    write everything to outfile in ascii format
    """ 
    store = pubStore.PubWriterFile(outFile)

    i = 0
    inRows = list(maxCommon.iterTsvRows(inIndexFile))
    doi2pmid = None
    logging.info("Converting %d files" % len(inRows))
    convCount = 0
    for row in inRows:
        # read line
        i+=1
        articleId, baseDir = row.articleId, row.baseDir
        zipFilename, filename = row.zipFilename, row.filename
        articleId=int(articleId)

        # open file from zipfile
        fullZipPath = join(baseDir, zipFilename)
        zipFile = zipfile.ZipFile(fullZipPath)
        logging.debug("Parsing %s, file %s, %d files left" % (fullZipPath, filename, len(inRows)-i))
        if doi2pmid==None:
            doi2pmid = parseDoi2Pmid(baseDir)
        xmlString = zipFile.open(filename).read()
        xmlTree   = pubXml.etreeFromXml(xmlString)

        # parse xml
        articleData = pubStore.createEmptyArticleDict()
        articleData = parseElsevier(xmlTree, articleData)
        if articleData==None:
            logging.warn("Parser got not data for %s" % filename)
            continue
        articleData["origFile"]="consyn://"+zipFilename+"/"+filename
        if articleData["doi"] in doi2pmid:
           articleData["pmid"] = doi2pmid[articleData["doi"]]

        pii = splitext(basename(filename))[0]
        articleData["externalId"]="EPII_"+pii
        articleData["fulltextUrl"]="http://www.sciencedirect.com/science/svapps/pii/"+pii

        # convert to ascii
        asciiString, mimeType = treeToAscii_Elsevier(xmlTree)
        if asciiString==None:
            logging.warn("No ASCII for %s / %s" % (zipFilename, filename))
            continue
        store.writeArticle(articleId, articleData)

        # write to output
        fileData = createFileData(articleData, mimeType, asciiString)
        store.writeFile(articleId, (1000*(articleId))+1, fileData, externalId=articleData["externalId"])
        convCount += 1
    logging.info("Converted %d files" % convCount)
    store.close()

def createChunksSubmitJobs(inDir, outDir, minId, chunkCount, maxJobs):
    """ convert Consyn ZIP files from inDir to outDir 
        split files into chunks and submit chunks to cluster system
    """
    maxCommon.mustExistDir(outDir)

    updateId, minId, alreadyDoneFiles = pubStore.parseUpdatesTab(outDir, minId)
    chunkSize  = pubStore.guessChunkSize(outDir)
    finalOutDir= outDir
    outDir     = tempfile.mktemp(dir = outDir, prefix = "temp.pubConvElsevier.update.")
    os.mkdir(outDir)
    chunkCount = None

    inFiles = os.listdir(inDir)
    inFiles = [x for x in inFiles if x.endswith(".ZIP")]
    # keep order of input of input files for first run
    if len(alreadyDoneFiles)!=0:
        processFiles = set(inFiles).difference(alreadyDoneFiles)
    else:
        processFiles = inFiles

    if len(processFiles)==0:
        logging.info("All updates done, not converting anything")
        return None

    indexFilename = join(outDir, "%d_index.tab" % updateId)
    maxArticleId  = createIndexFile(inDir, processFiles, indexFilename, updateId, minId, chunkCount, chunkSize)
    indexSplitDir = indexFilename+".tmp.split"
    pubStore.splitTabFileOnChunkId(indexFilename, indexSplitDir)
    submitJobs(indexSplitDir, outDir, maxJobs)

    pubStore.moveFiles(outDir, finalOutDir)
    shutil.rmtree(outDir)
    #shutil.rmtree(indexSplitDir)
    pubStore.appendToUpdatesTxt(finalOutDir, updateId, maxArticleId, processFiles)

# this is a job script, so it is calling itself via parasol/bsub/qsub
if __name__=="__main__":
    parser = optparse.OptionParser("""usage: %prog [options] <inIndexFile> <outFile> - job script to convert a Elsevier fulltext file (given using an index file) from consyn format to ascii""")
    parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages")
    (options, args) = parser.parse_args()
    if args==[]:
        parser.print_help()
        exit(1)

    inIndexFile, outFile = args
    pubGeneric.setupLogging(__file__, options)
    convertOneChunk(inIndexFile, outFile)

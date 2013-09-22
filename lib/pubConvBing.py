import zipfile, glob, logging, sys, atexit, shutil, gzip, re, HTMLParser, datetime, operator
import lxml.html
import unidecode
from lxml.html.clean import Cleaner
from os.path import *
from collections import defaultdict

import pubGeneric, maxCommon, pubGeneric, pubStore, pubConvElsevier, pubXml, pubCompare

def indexTsv(zipFname, tsvName, outFname):
    """ unzip a zipfile, recompress all the tsvs inside 
    with gzip and create an .index.gz for them"""

    #def indexTsv(zipFname, tsvName, outFname, bgzipPath):

    # extract to local disk
    tmpDir = pubGeneric.makeTempDir("bingData")
    maxCommon.delOnExit(tmpDir)
    logging.info("Extracting to %s" % tmpDir)
    cmd =["unzip", "-d",tmpDir, zipFname]
    maxCommon.runCommand(cmd)

    tempFname = join(tmpDir, tsvName)
    logging.info("Indexing %s to %s" % (tempFname, outFname))
    # index lines
    ofh = gzip.open(outFname, "w")
    ifh = open(tempFname, "rb")
    offset = 0
    # the file iterator does not work  with tell()!!
    #for line in ifh:
    while True:
        line = ifh.readline()
        if line=="":
            break
        url = line[0:line.find("\t")]
        ofh.write("%s\t%d\n" % (url, offset))
        #logging.debug("url %s, offset %d" % (url, offset))
        offset = ifh.tell()
    ofh.close()

    # re-compress with gzip
    tmpFnames = glob.glob(join(tmpDir, "*.tsv"))
    assert(len(tmpFnames)==1)
    tmpFname = tmpFnames[0]
    zipDir = dirname(zipFname)
    finalFname = join(zipDir, tsvName+".gz")
    logging.info("Compressing to %s" % finalFname)
    #cmd = "%s %s -c > %s" % (bgzipPath, tmpFname, finalFname)
    cmd = "gzip %s -c > %s" % (tmpFname, finalFname)
    maxCommon.runCommand(cmd)
    shutil.rmtree(tmpDir)
        
def rewriteIndexesFindDuplicates(inDir):
    """
      read all index.gz files, sorted by date, mark all duplicated urls with ! as the first 
      letter. Mark all but the last occurence.
    """
    # reverse-sort filenames by date
    indexFnames = glob.glob(join(inDir, "*.tsv.index.gz"))
    timedFnames = []
    for fn in indexFnames:
        dateStr = basename(fn).split(".")[3]
        dateObj = datetime.datetime.strptime(dateStr, "%Y-%m-%d").date()
        timedFnames.append( (fn, dateObj))
    timedFnames.sort(key=operator.itemgetter(1), reverse=True)
    sortedFnames = [fn for fn, d in timedFnames]

    # mark duplicate urls with !
    urlsSeen = set()
    urlCount = 0
    urlRemoved = 0
    for fname in sortedFnames:
        logging.debug("Reading %s" % fname)
        newRows = []
        for line in gzip.open(fname).read().splitlines():
            urlCount +=1
            url, count = line.rstrip("\n").split("\t")
            if url in urlsSeen:
                url = "!"+url
                urlRemoved += 1
            newRows.append((url, count))
            urlsSeen.add(url)

        shutil.move(fname, fname+".bak")
        ofh = gzip.open(fname, "w")
        for row in newRows:
            ofh.write("\t".join(row))
            ofh.write("\n")
        ofh.close()
        logging.info("Wrote %s" % fname)
    logging.info("Seen %d URLs, removed %d" % (urlCount, urlRemoved))


def createIndexJobs(runner, inDir):
    """ 
        submit jobs to cluster to index tsv files within zip files 
    """
    zipFnames = glob.glob(join(inDir, "*.zip"))
    # keep order of input of input files for first run

    doneIndices = glob.glob(join(inDir, "*.tsv.index.gz"))
    doneIndices = set([".".join(basename(x).split(".")[:-2]) for x in doneIndices])

    for zipFname in zipFnames:
        zipDir = dirname(zipFname)
        tsvNames = zipfile.ZipFile(zipFname).namelist()
        for tsvName in tsvNames:
            if tsvName in doneIndices:
                logging.info("Already indexed: %s" % tsvName)
                continue
            outFname = join(zipDir, basename(tsvName)+".index.gz")
            #bgzipPath = maxCommon.which("bgzip")
            params = [zipFname, tsvName, outFname]
            #params = [zipFname, tsvName, outFname, bgzipPath]
            runner.submitPythonFunc("pubConvBing.py", "indexTsv", params)
    runner.finish()
            
    #pubStore.appendToUpdatesTxt(finalOutDir, updateId, maxArticleId, processFiles)

#def concatUrls(inDir, outDir, outFname):
#    " concat all ids of id files in inDir to outFname "
#    outPath = join(outDir, outFname)
#    inMask = join(inDir, "*_ids.tab")
#    idFnames = glob.glob(inMask)
#    logging.debug("Concatting urls from %s to %s" % (inMask, outPath))
#    ids = []
#    for inFname in idFnames:
#        for row in maxCommon.iterTsvRows(inFname):
#            ids.append(row.id)
#    ofh = open(outPath, "w")
#    ofh.write("#id\n")
#    for url in urls:
#        ofh.write("%s\n" % url)
#    ofh.close()
#    return outPath

def submitConvertJobs(runner, zipDir, updateId, chunkIds, splitDir, idFname, outDir):
    for chunkId in chunkIds:
        chunkFname = join(splitDir, str(chunkId))
        outFname = join(outDir, str(updateId)+"_"+str(chunkId)+".articles.gz")
        maxCommon.mustNotExist(outFname)
        thisFilePath = __file__
        params = [zipDir, idFname, chunkFname, "{check out exists %s}" % outFname]
        #command = "%s %s %s {check in line %s} {check in line %s} {check out exists+ %s}" % (sys.executable, thisFilePath, zipDir, idFname, chunkFname, outFname)
        #runner.submit(command)
        runner.submitPythonFunc("pubConvBing.py", "convertOneChunk", params)
    runner.finish(wait=True)

def createChunksSubmitJobs(inDir, outDir, minId, runner, chunkSize):
    tmpDir = pubGeneric.makeTempDir("bingData", tmpDir=outDir)
    #maxCommon.delOnExit(tmpDir)

    maxCommon.mustExistDir(outDir)
    updateId, minId, alreadyDoneFiles = pubStore.parseUpdatesTab(outDir, minId)
    # get all .gz.index files, remove the already done files
    inFnames = glob.glob(join(inDir, "*.index.gz"))
    inBaseNames = set([basename(x) for x in inFnames])
    todoBasenames = inBaseNames - set(alreadyDoneFiles)
    todoFnames = [join(inDir, x) for x in todoBasenames]
    if len(todoFnames)==0:
        logging.info("All input files already converted")
        return

    indexFilename = join(outDir, "%d_index.tab" % updateId)
    indexFile = open(indexFilename, "w")
    headers = ["articleId", "tsvFile", "url", "offset"]
    indexFile.write("\t".join(headers))
    indexFile.write("\n")

    # read them and create a big index file:
    # with tsvname, url, offset
    numId = minId
    doneUrls = set()
    for fname in todoFnames:
        baseName = basename(fname)
        for line in gzip.open(fname):
            url, offset = line.rstrip("\n").split("\t")
            assert(offset.isdigit())
            if "\t" in url or "\n" in url:
                logging.info("tab or NL in url %s, skipping" % url)
                continue
            if url in doneUrls:
                logging.info("Already did %s" % url)
                continue
            baseName = baseName.replace(".index.gz", ".gz")
            row = [str(numId), baseName, url, offset]
            indexFile.write("\t".join(row))
            indexFile.write("\n")
            numId+=1
    indexFile.close()

    # split the index file into chunks, one per job
    chunkIds = pubStore.splitTabFileOnChunkId(indexFilename, tmpDir, chunkSize=chunkSize)
    idFname  = pubGeneric.concatIdentifiers(outDir, tmpDir, "doneArticles.tab")
    # submit one conversion job per chunk
    submitConvertJobs(runner, inDir, updateId, chunkIds, tmpDir, idFname, tmpDir)
    pubGeneric.concatDelIdFiles(tmpDir, outDir, "%d_ids.tab" % updateId)
    pubGeneric.concatDelLogs(tmpDir, outDir, "%d.log" % updateId)
    pubStore.moveFiles(tmpDir, outDir)
    shutil.rmtree(tmpDir)
    pubStore.appendToUpdatesTxt(outDir, updateId, numId, todoBasenames)

def diacToUnicode(textOrig):
    """ in theory DC uses standard html diacritics
    http://dublincore.org/documents/2000/07/16/usageguide/simple-html.shtml#one
    but nature does it like this 
    Bj|[ouml]|rn Bauer
    we support both
    """
    if "&" not in textOrig and "|" not in textOrig:
        return textOrig
    text = textOrig.replace("|[", "&").replace("]|", ";")
    #text = lxml.html.fromstring(text).text
    h = HTMLParser.HTMLParser()
    text = h.unescape(text)
    logging.debug("Converted %s to %s" % (textOrig, text))
    return text

def parseMetaData(metaTags, artDict):
    " parse dublin core or prism metadata out of html head elements "
    authors = []
    for metaTag in metaTags:
        attribs = metaTag.attrib
        if not ("name" in attribs and "content" in attribs):
            continue
        
        name = attribs["name"]
        content = attribs["content"]
        name = name.lower()
        # PRISM and open graph meta data
        if name=="og.type":
            artDict["articleTypE"] = content.strip()
        if name=="og.description":
            artDict["abstract"] = content.strip()
        if name=="prism.volume" or name=="citation_volume":
            artDict["vol"] = content
        if name=="prism.number" or name=="citation_issue":
            artDict["issue"] = content
        if name=="prism.startingPage" or name=="citation_firstpage":
            artDict["page"] = content
        if name=="prism.publicationName" or name=="citation_journal_title" or name=="og.title":
            artDict["journal"] = content
        if name=="prism.issn":
            artDict["printIssn"] = content
        if name=="prism.eIssn":
            artDict["eIssn"] = content
        if name=="citation_doi":
            artDict["doi"] = content.replace("doi:", "")
        if name=="citation_issn":
            if artDict.get("printIssn","")=="":
                artDict["printIssn"] = content
        if name=="citation_authors":
            artDict["authors"] = content
        if name=="citation_pmid":
            artDict["pmid"] = content
        if name=="citation_abstract_html_url":
            if "PMC" in content:
                artDict["pmcId"] = content.split("/")[2].replace("PMC","")
        if name=="citation_section":
            artDict["articleType"] = content
        # DUBLIN CORE metadata
        if name=="dc.date":
            parts = re.split("[-/ ]", content)
            #artDict["year"] = content.split("-")[0].split(" ")[0]
            for p in parts:
                if len(p)==4:
                    artDict["year"] = p
        if name=="dc.title":
            artDict["title"] = diacToUnicode(content)
        if name=="dc.creator" or name=="dc.contributor" or name=="citation_author":
            content = diacToUnicode(content)
            if "," not in content:
                # inverse firstname lastname to lastname, firstname
                content = content.strip()
                parts = content.split()
                if len(parts)>1:
                    authors.append(parts[-1]+", "+" ".join(parts[:-1]))
                else:
                    authors.append(content)
            else:
                authors.append(content)
        if name=="dc.identifier":
            if content.startswith("doi:"):
                artDict["doi"] = content.replace("doi:","")
            if content.startswith("pmid:"):
                artDict["pmid"] = content.replace("pmid:","")
    if len(authors)!=0 and artDict.get("authors", "")=="":
        artDict["authors"] = "; ".join(authors)
    return artDict

def minimalHtmlToDicts(url, content):
    " a minimalistic article dict filler, does not try to parse the html "
    logging.debug("Falling back to minimal html to text")
    fileDict = pubStore.createEmptyFileDict(url=url, content=content, mimeType="text/html")
    fileDict = pubGeneric.toAsciiEscape(fileDict, mimeType="text/html")
    if fileDict==None or not "content" in fileDict:
        return None, None
    text = fileDict["content"]
    title = unidecode.unidecode(content[:100])
    abstract = unidecode.unidecode(content[100:1000])
    artDict = pubStore.createEmptyArticleDict(source="bing", fulltextUrl=url, \
        title=title, abstract=abstract)
    #if fileDict==None: #continue
    return artDict, fileDict

def convertMicrosoft(content):
    content = content.replace("#N#", "\n") # Microsoft replaces special chars
    content = content.replace("#R#", "\n") # why oh why?
    content = content.replace("#M#", "\n") # 
    content = content.replace("#TAB#", " ") # 
    return content

def convertHtmlToDicts(url, content):
    """ given a url and content, create file and article dictionaries 
    content has to include normal newlines, no \a or #N# replacers

    returns None, None on error
    
    """
    # lxml does not like unicode if the document has an explicit encoding
    if " encoding=" not in content:
        content = pubGeneric.forceToUnicode(content)
    logging.debug("Converting to text: %s " % (repr(url)))
    artDict = pubStore.createEmptyArticleDict(source="bing", fulltextUrl=url)

    if not "<html" in content:
        return None, None

    try:
        logging.debug("Parsing html with lxml, html size %d" % len(content))
        tree = lxml.html.document_fromstring(content)
        logging.debug("end parse html")
    except lxml.etree.XMLSyntaxError:
        return None, None

    titleEl = tree.find("head/title")
    if titleEl!=None:
        title = titleEl.text
    else:
        logging.debug("No title found?")
        title = ""
        
    metaTags = tree.findall("head/meta")
    artDict = parseMetaData(metaTags, artDict)
    logging.debug("Cleaning html tree")
    cleaner = Cleaner()
    cleaner.javascript = True
    cleaner.style = True
    cleaner.meta = True
    cleaner.embedded = True
    cleaner.page_structure=True 
    #cleaner.remove_tags = ["a", "li", "td"]
    cleanTree = cleaner.clean_html(tree)
    logging.debug("Cleaning done, now converting to ASCII")
    #text = cleanTree.text_content()
    newlineTags = ["p", "br"]
    asciiText = pubXml.treeToAsciiText(cleanTree, newlineTags)
    logging.debug("ASCII conversion done")
    logging.debug("title: %s" % title)

    if "title" not in artDict or artDict["title"]=="":
        artDict["title"] = title

    if artDict["abstract"]=="":
        abstract = unidecode.unidecode(asciiText[0:1500]).strip()
        artDict["abstract"] = abstract

    logging.debug("abstract: %s" % artDict["abstract"])
    fileDict = pubStore.createEmptyFileDict(url=url, content=asciiText, mimeType="text/html")
    logging.debug("meta data extract success: %s" % artDict)
    return artDict, fileDict
        
def convertOneChunk(gzDir, idFname, inIndexFile, outFile):
    # for each row in index:
    store = pubStore.PubWriterFile(outFile)
    donePiis = pubGeneric.parseDoneIds(idFname)

    # log to file
    outBase = join(dirname(outFile), basename(outFile).split(".")[0])
    logFname = outBase+".log"
    pubGeneric.setupLogging(__file__, None, logFileName=logFname)

    idFname = outBase+"_ids.tab"
    logging.debug("Writing ids to %s" % idFname)
    idFh = open(idFname, "w")
    idFh.write("#articleId\texternalId\n")

    lastTsvFname = None
    tsvFile = None
    pmidFinder = pubCompare.PmidFinder()
    for row in maxCommon.iterTsvRows(inIndexFile, encoding=None):
        # open file and seek, if necessry
        if tsvFile==None or lastTsvFname!=row.tsvFile:
            logging.debug("Seeking to %s in tsvfile %s" % (row.offset, row.tsvFile))
            tsvFile = gzip.open(join(gzDir, row.tsvFile))
            tsvFile.seek(int(row.offset))
        lastTsvFname = row.tsvFile

        line = tsvFile.readline()

        if row.url.startswith("!"):
            logging.info("Ignoring %s, marked as duplicated" % row.url)
            continue
        #fields are: ["articleId", "tsvFile", "url", "offset"]
        #print "line", line[:20]
        fields = line.split("\t")
        url = fields[0]
        logging.debug("Replacing weird bing chars")
        content = fields[-1]
        #print "urls", repr(url), repr(row.url)
        assert(url==row.url)
        assert(len(content)!=0)
        url = url.decode("utf8")

        logging.debug("Converting to text")
        content = convertMicrosoft(content)
        artDict, fileDict = convertHtmlToDicts(url, content)
        if artDict==None:
            artDict, fileDict = minimalHtmlToDicts(url, content)
        if artDict==None:
            continue
        artDict["pmid"]  = pmidFinder.lookupPmid(artDict)
        # write file
        articleId = int(row.articleId)
        fileId = articleId*1000
        store.writeFile(articleId, fileId, fileDict)
        store.writeArticle(articleId, artDict)
    store.close()
    

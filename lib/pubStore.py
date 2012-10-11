# Classes to read and write documents to directories

# gzipfiles are stored in "chunks" of x documents, usually to get around 2000 
# chunks per dataset

# reader class gets path/xxxx and will open path/xxxx.articles.gz and path/xxxx.files.gz
# reader class yields files from these two files

# writer class writes to local filesystem, as two different textfiles:
# xxxx.files, xxxx.articles
# then gzips them and copies gzipfiles to shared cluster filesystem

import os, logging, sys, collections, time, codecs, shutil, tarfile, csv, glob, operator
import zipfile, gzip, re, sqlite3
import pubGeneric, pubConf, maxCommon, pubStore, unicodeConvert, maxTables

from os.path import *

# need to increase maximum size of fields for csv module
csv.field_size_limit(50000000)

# DATA FIELD DEFINITIONS

articleFields=[
"articleId",  # internal number that identifies this article in the pubtools system
"externalId", # original string id of the article, e.g. PMC12343 or doi:123213213/dfsdf or PMID123123
"source",  # the origin of the article, something like "elsevier" or "pubmed" or "medline"
"origFile", # the original file where the article came from, e.g. the zipfile or genbank file
"journal",      # journal or book title
"printIssn",    # ISSN of the print edition of the article
"eIssn",        # optional: ISSN of the electronic edition of the journal/book of the article
"journalUniqueId", # medline only: NLM unique ID for journal
"year",         # first year of publication (electronic or print or advanced access)
"articleType", # research-article, review or other
"articleSection",  # elsevier: the section of the book/journal, e.g. "methods", "chapter 5" or "Comments" 
"authors",  # list of author names, usually separated by semicolon
"authorEmails",  # email addresses of authors
"authorAffiliations",  # authors' affiliations
"keywords", # medline: mesh terms or similar, separated by / (medline is using , internally)
"title",    # title of article
"abstract", # abstract if available
"vol",      # volume
"issue",    # issue
"page",            # first page of article, can be ix, x, or S4
"pmid",            # PubmedID if available
"pmcId",           # Pubmed Central ID
"doi",             # DOI, without leading doi:
"fulltextUrl",     # URL to fulltext of article
"time"     # date of download
]

fileDataFields = [
"fileId", # numerical ID of the file: its article (ID * 1000)+some count (for suppl files)
"externalId", # copy of external ID, for quick greps and mallet
"articleId", # numerical ID of the article
"url", # the url where the file is located, can also be elsevier://, pmcftp:// etc
"desc", # a description of the file, e.g. main text, html title or supp file description
"fileType", # can be either "main" or "supp"
"time", # time/day of conversion from PDF/html/etc
"mimeType", # mimetype of original file before to-text-conversion
"content" # the data from this file (newline => \a, tab => space, cr => space, \m ==> \a)
]

ArticleRec = collections.namedtuple("ArticleRec", articleFields)
emptyArticle = ArticleRec(*len(articleFields)*[""])

FileDataRec = collections.namedtuple("FileRecord", fileDataFields)
emptyFileData = FileDataRec(*len(fileDataFields)*[""])

def createEmptyFileDict(url=None, time=time.asctime(), mimeType=None, content=None, fileType=None, desc=None):
    fileData = emptyFileData._asdict()
    if time!=None:
        fileData["time"]=time
    if url!=None:
        fileData["url"]=url
    if mimeType!=None:
        fileData["mimeType"]=mimeType
    if content!=None:
        fileData["content"]=content
    if fileType!=None:
        fileData["fileType"]=fileType
    if desc!=None:
        fileData["desc"]=desc
    logging.log(5, "Create new file record, url=%s, fileType=%s, desc=%s" % (url, fileType, desc))
    return fileData

def createEmptyArticleDict(pmcId=None, source=None, externalId=None, journal=None, id=None, origFile=None, authors=None, fulltextUrl=None, keywords=None, title=None, abstract=None):
    """ create a dictionary with all fields of the ArticleType """
    metaInfo = emptyArticle._asdict()
    metaInfo["time"]=time.asctime()
    if pmcId:
        metaInfo["pmcId"]=pmcId
    if origFile:
        metaInfo["origFile"]=origFile
    if source:
        metaInfo["source"]=source
    if journal:
        metaInfo["journal"]=journal
    if title:
        metaInfo["title"]=title
    if authors:
        metaInfo["authors"]=authors
    if fulltextUrl:
        metaInfo["fulltextUrl"]=fulltextUrl
    if keywords:
        metaInfo["keywords"]=keywords
    if externalId:
        metaInfo["externalId"]=externalId
    if abstract:
        metaInfo["abstract"]=abstract
    return metaInfo

def splitTabFileOnChunkId(filename, outDir):
    """ 
    use the chunkId field of a tab-sep file as the output filename 
    """
    if isdir(outDir):
        logging.info("Deleting %s" % outDir)
        shutil.rmtree(outDir)

    if not os.path.isdir(outDir):
        logging.info("Creating directory %s" % outDir)
        os.makedirs(outDir)
    maxCommon.mustBeEmptyDir(outDir)

    # read data into data dict and split by "chunkId" field
    headerLine = open(filename).readline()
    logging.info("Reading %s, splitting into pieces" % filename)
    data = {}
    for row in maxCommon.iterTsvRows(filename):
        chunkId = row.chunkId
        data.setdefault(chunkId, []).append("\t".join(row)+"\n")

    # write to outDir
    logging.info("Splitting file data, Writing to %d files in %s/xxxx.tgz" % (len(data), outDir))
    pm = maxCommon.ProgressMeter(len(data))
    for chunkIdString, lines in data.iteritems():
        outfname = os.path.join(outDir, chunkIdString)
        logging.debug("Writing to %s" % outfname)
        fh = codecs.open(outfname, "w", encoding="utf8")
        fh.write(headerLine)
        for line in lines:
            fh.write(line)
        fh.close()
        pm.taskCompleted()

    return data.keys()

def toUnicode(var):
    " force variable to unicode, somehow "
    if type(var)==type(1):
        var = unicode(var)
    if var==None:
        var = "NotSpecified"
    elif type(var)==type(unicode()):
        pass
    else:
        try:
            var = var.decode("utf8")
        except UnicodeDecodeError, msg:
            logging.debug("Could not decode %s as utf8, error msg %s" % (var, msg))
            var = var.decode("latin1")
    return var

def listToUtf8Escape(list):
    """ convert list of variables to utf8 string as well as possible and replace \n and \t"""
    utf8List=[]
    for var in list:
        var = toUnicode(var)
        var = replaceSpecialChars(var)
        utf8List.append(var)
    return utf8List

def dictToUtf8Escape(dict):
    """ convert dict of variables to utf8 string as well as possible and replace \n and \t"""
    utf8Dict={}
    for key, var in dict.iteritems():
        var = toUnicode(var)
        var = replaceSpecialChars(var)
        utf8Dict[key]=var
    return utf8Dict

def utf8GzReader(fname):
    " wrap a utf8 codec around a gzip reader "
    if fname=="stdin":
        return sys.stdin
    zf = gzip.open(fname, "rb")
    reader = codecs.getreader("utf-8")
    fh = reader(zf)
    return fh

def utf8GzWriter(fname):
    " wrap a utf8 codec around a gzip writer "
    if fname=="stdout":
        return sys.stdout
    zf = gzip.open(fname, "w")
    reader = codecs.getwriter("utf-8")
    fh = reader(zf)
    return fh

class PubWriterFile:
    """ 
    a class that stores article and file data into tab-sep files
    that are located in a subdirectory. 
    
    Constructor will create two files:
        <chunkId>.files.gz    = raw file content and some meta (filetype, url)
        <chunkId>.articles.gz = article meta data (author, year, etc)

    The constructor takes only the .fileData base filename as input and will
    derive outDir and chunkId from it.

    all writes will first go to tempDir, and will only be copied over 
    to outDir on .close()

    """
    def __init__(self, fileDataFilename, compress=True):
        self.articlesWritten = 0
        self.filesWritten = 0
        tempDir = pubConf.getTempDir()

        # very convoluted way to find output filenames
        # needed because of parasol 
        outDir = os.path.dirname(fileDataFilename)
        fileDataBasename = os.path.basename(fileDataFilename)
        chunkId = fileDataBasename.split(".")[0]
        fileBaseName = chunkId+".files"
        articleBaseName = chunkId+".articles"

        self.finalArticleName = join(outDir, articleBaseName+".gz")
        self.finalFileDataName    = join(outDir, fileBaseName+".gz")
        fileFname = os.path.join(tempDir, fileBaseName)
        #self.fileFh = writeUtf8Gz(fileFname)# DOES NOT WORK ... ?
        #self.articleFh = writeUtf8Gz(articleFname) # DOES NOT WORK ... ?
        self.fileFh = codecs.open(fileFname, "w", encoding="utf8")
        articleFname = os.path.join(tempDir, articleBaseName)
        self.articleFh = codecs.open(articleFname, "w", encoding="utf8") 

        self._writeHeaders()

        self.outFilename = os.path.join(outDir, fileDataBasename)

    def _writeHeaders(self):
        """ write headers to output files """
        logging.debug("Writing headers to output files, %s and %s" % (self.fileFh.name, self.articleFh.name))
        self.articleFh.write("#"+"\t".join(articleFields)+"\n")
        self.fileFh.write("#"+"\t".join(fileDataFields)+"\n")

    def _removeSpecChar(self, lineDict):
        " remove tab and NL chars from values of dict "
        #newDict[key] = val.replace("\t", " ").replace("\n", " ")
        # RAHHH! CRAZY UNICODE LINEBREAKS:
        newDict = {}
        for key, val in lineDict.iteritems():
            newDict[key] = " ".join(unicode(val).splitlines())
        return newDict
        
    def writeFile(self, articleId, fileId, fileDict, externalId=""):
        """ appends id and data to current .file table,
            will not write if maximum filesize exceeded
        """
        if fileDict["content"]==None:
            logging.warn("file %s, object or content is None" % fileId)
            fileDict["content"] = ""

        if len(fileDict["content"]) > pubConf.MAXTXTFILESIZE:
            logging.info("truncating file %s, too big" % fileId)
            fileDict["content"] = fileDict["content"][:pubConf.MAXTXTFILESIZE]

        if len(fileDict)!=len(fileDataFields):
            logging.error("column counts between file dict and file objects don't match")
            dictFields = fileDict.keys()
            dictFields.sort()
            logging.error("columns are          %s" % str(dictFields))
            expFields = fileDataFields
            expFields.sort()
            logging.error("expected columns are %s" % str(expFields))
            sys.exit(1)

        fileDict["externalId"] = externalId
        fileDict["fileId"]=str(fileId)
        fileDict["articleId"]=str(articleId)
        # convert dict to line and write to xxxx.file 
        fileTuple = FileDataRec(**fileDict)
        fileTuple = listToUtf8Escape(fileTuple)
        line = "\t".join(fileTuple)
        self.filesWritten += 1
        logging.log(5, "Writing line to file table")
        self.fileFh.write(line+"\n")
        
    def writeArticle(self, articleId, articleDict):
        """ appends data to current chunk """
        articleDict["articleId"]=articleId
        articleDict = self._removeSpecChar(articleDict)
        #logging.log(5, "appending metaInfo %s to %s" % (str(articleDict), path))
        if len(articleDict)!=len(articleFields):
            logging.error("column counts between article dict and article objects don't match")
            dictFields = articleDict.keys()
            dictFields.sort()
            logging.error("columns are          %s" % str(dictFields))
            expFields = articleFields
            expFields.sort()
            logging.error("expected columns are %s" % str(expFields))
            sys.exit(1)

        self.articlesWritten += 1
        articleTuple = ArticleRec(**articleDict)

        # convert all fields to utf8 string, remove \n and \t
        articleTuple = listToUtf8Escape(articleTuple)

        line = "\t".join(articleTuple)
        self.articleFh.write(line+"\n")
        
    def _gzipAndMove(self, fname, finalName):
        " gzip fname and move to finalName "
        gzName = fname+".gz"
        if isfile(gzName):
            os.remove(gzName)
        maxCommon.runCommand("gzip %s" % fname)
        shutil.copyfile(gzName, finalName)
        os.remove(gzName)

    def close(self, keepEmpty=False):
        """ 
        close the 3 files, copy them over to final targets and  delete the
        temps 
        """ 
        self.fileFh.close()
        self.articleFh.close()

        #if self.compress:
            #assert(self.outFilename.endswith(".zip"))

            # only copy a file if it contains data
            #filenames = []
            #if self.articlesWritten > 0:
                #filenames.append(self.articleFh.name)
            #if self.filesWritten > 0:
                #filenames.append(self.fileFh.name)

            #logging.debug("Compressing files %s to %s" % (filenames, self.outFilename))
            #zipFile = zipfile.ZipFile(self.outFilename, mode='w', compression=zipfile.ZIP_DEFLATED)
            #for fn in filenames:
                #zipFile.write(fn, arcname=os.path.basename(fn))
            #zipFile.close()
        #else:
        logging.debug("Copying local tempfiles over to files on server")
        assert(self.fileFh.name.endswith(".files"))
        if self.articlesWritten > 0 or keepEmpty:
            self._gzipAndMove(self.articleFh.name, self.finalArticleName)
        if self.filesWritten > 0 or keepEmpty:
            logging.debug("compressing and copying files table")
            self._gzipAndMove(self.fileFh.name, self.finalFileDataName)

def createPseudoFile(articleData):
    """ create a file from the abstract and title of an article,
    for articles that don't have fulltext (pubmed) """
    logging.debug("no file data, creating pseudo-file from abstract")
    fileData = createEmptyFileDict()
    fileData["url"] = articleData.fulltextUrl
    fileData["content"] = " ".join([articleData.title, articleData.abstract])
    fileData["mimeType"] = "text/plain"
    fileData["fileId"] = int(articleData.articleId) * (10**pubConf.FILEDIGITS)
    fileTuple = FileDataRec(**fileData)
    return fileTuple

class PubReaderFile:
    """ 
    read articles from tab-sep files, optionally compressed 
    """
    def __init__(self, fname):
        " fname can end in .articles.gz, reader will still read both articles and files "
        logging.debug("Reading data from file with prefix %s (.articles.gz, .files.gz)" % fname)
        baseDir = dirname(fname)
        base = basename(fname).split('.')[0]
        articleFn = join(baseDir, base+".articles.gz")
        fileFn = join(baseDir, base+".files.gz")
        logging.debug("Reading %s and %s" % (articleFn, fileFn))

        self.articleRows = None
        if isfile(articleFn):
            self.articleRows = maxCommon.iterTsvRows(articleFn, encoding="utf8")
                
        self.fileRows = None
        if isfile(fileFn)!=None:
            self.fileRows  = maxCommon.iterTsvRows(fileFn, encoding="utf8")

        assert(self.articleRows!=None or self.fileRows!=None)

    def iterFileRows(self):
        """ iterate over file data """
        return self.fileRows

    def iterArticleRows(self):
        """ iterate over article data """
        return self.articleRows

    def _readFilesForArticle(self, articleId, fileDataList):
        " reads files until the articleId changes, adds them to fileDataList "

        #newFileDataList = [fd for fd in fileDataList if fileDataList.articleId=articleId]
        #if len(newFileDataList)<>len(fileDataList):
            #logging.debug("Skipped %d files
        
        for fileData in self.fileRows:
           logging.debug("Read file data %s for article %s" % \
               (str(fileData.fileId), fileData.articleId))
           text = pubGeneric.forceToUnicode(fileData.content)
           fileData = fileData._replace(content=text)
           if articleId==fileData.articleId:
               logging.debug("adding file data")
               fileDataList.append(fileData)
           else:
               fileIds = list(set([str(x.fileId)[:pubConf.ARTICLEDIGITS] for x in fileDataList]))
               logging.debug("article change. yielding: articleId %s, %d files with ids %s" % \
                   (articleId, len(fileDataList), fileIds))
               assert(len(fileIds)==1)
               assert(fileIds[0]==str(articleId))
               return fileDataList, fileData
        return fileDataList, None

    def _keepBestMain(self, files):
        " if there is a PDF and XML version for the main text, remove the PDF. keep all suppl files "
        mainFiles = {}
        newFiles = []
        for fileData in files:
            if fileData.fileType=="main":
                assert(fileData.mimeType not in mainFiles) # we should never have two main files with same type
                mainFiles[fileData.mimeType] = fileData
            else:
                newFiles.append(fileData)

        if "application/pdf" in mainFiles and "text/xml" in mainFiles:
            del mainFiles["application/pdf"]

        assert(len(mainFiles)==1)
        newFiles.insert(0, mainFiles.values()[0])
        return newFiles

    def iterArticlesFileList(self, onlyMeta, onlyBestMain):
        """ iterate over articles AND files, as far as possible

        for input files with article and file data:
            yield a tuple (articleData, list of fileData) per article 
        for input files with no article data, yield a tuple (None, [fileData])
        for input files with no file data, generate pseudo-file from abstract (title+abstract+authors)
        if onlyMeta is True, do not read .files.gz and yield (articleData, pseudoFile) 
        if onlyBestMain is True, ignore the PDF file if there are PDF and XML main files
        """
        fileDataList = []
        lastFileData = None
        for articleData in self.articleRows:
            if len(fileDataList)!=0 and fileDataList[0].articleId!=articleData.articleId:
                logging.warn("skipping %s, seems that articleId is out of sync with files" %
                    articleData.articleId)
                continue
            logging.debug("Read article meta info for %s" % str(articleData.articleId))

            if self.fileRows!=None and not onlyMeta==True:
                # if file data is there and we want it, read as much as we can
                fileDataList, lastFileData = self._readFilesForArticle(articleData.articleId, fileDataList)
                if onlyBestMain:
                    fileDataList = self._keepBestMain(fileDataList)
                yield articleData, fileDataList
                fileDataList = [lastFileData]
            else:
                # if only abstract: create pseudo file (medline)
                fileTuple = createPseudoFile(articleData)
                yield articleData, [fileTuple]

        if len(fileDataList)!=0 and lastFileData!=None:
            logging.debug("last line: yielding last article + rest of fileDataList")
            yield articleData, fileDataList
        #if self.articleRows!=None:
        #else:
        #for fileData in self.fileRows:
        #logging.debug("no article data, yielding only file data for id %s" % fileData.fileId)
        #yield None, [fileData]

    def iterFileArticles(self):
        """ iterate over files and also give the articleData for each file """
        self.articleIdToData = {}
        # normal operation, if we have files
        if self.fileRows!=None:
            for articleData in self.articleRows:
                self.articleIdToData[articleData.articleId] = articleData

            for fileData in self.fileRows:
               articleData = self.articleIdToData.get(fileData.articleId, None)
               yield fileData, articleData
        # just return articles and blank file if we don't have files
        else:
            for articleData in self.articleRows:
                yield emptyFileData, articleData

    def iterFiles(self):
        """ compat to iterFileArticles, yields only Files """
        for fileData in self.fileRows:
           yield fileData, None

    def iterArticles(self):
        """ compat to iterFileArticles, yields only articles """
        for articleData in self.articleRows:
           yield None, articleData


    def close(self):
        if self.articleRows:
            self.articleRows.close()
        if self.fileRows:
            self.fileRows.close()

def iterArticleDataDir(textDir, type="articles", filterFname=None, updateIds=None):
    """ yields all articleData from all files in textDir 
        Can filter to yield only a set of filenames or files for a 
        given list of updateIds.
    """
    fcount = 0
    if type=="articles":
        baseMask = "*.articles.gz"
    elif type=="files":
        baseMask = "*.files.gz"
    elif type=="annots":
        baseMask = "*.tab.gz"
    else:
        logging.error("Article type %s not valid" % type)
        sys.exit(1)
        
    if isfile(textDir):
        fileNames = [textDir]
        logging.debug("Found 1 file, %s" % textDir)
    else:
        fileNames = glob.glob(os.path.join(textDir, baseMask))
        if updateIds!=None:
            logging.debug("Restricting fulltext files to updateIds %s" % str(updateIds))
            filteredFiles = set()
            for updateId in updateIds:
                filteredFiles.update([f for f in fileNames if f.startswith(str(updateId)+"_")])

        logging.debug("Found %d files in input dir %s" % (len(fileNames), textDir))

    pm = maxCommon.ProgressMeter(len(fileNames))
    for textFname in fileNames:
        if filterFname!=None and not filterFname in textFname:
            logging.warn("Skipping %s, because file filter is set" % textFname)
            continue
        reader = PubReaderFile(textFname)
        logging.debug("Reading %s, %d files left" % (textFname, len(fileNames)-fcount))
        fcount+=1
        if type=="articles":
            for articleData in reader.articleRows:
                yield articleData
        elif type=="files":
            for fileData in reader.fileRows:
                yield fileData
        elif type=="annots":
            for row in maxCommon.iterTsvRows(textFname):
                yield row
        else:
            assert(False) # illegal type parameter
        pm.taskCompleted()

all_chars = (unichr(i) for i in xrange(0x110000))
control_chars = ''.join(map(unichr, range(0,7) + range(8,32) + range(127,160)))
control_char_re = re.compile('[%s]' % re.escape(control_chars))

def replaceSpecialChars(string):
    " replace all special characters with space and linebreaks to \a = ASCII 7 = BELL"
    string = "\n".join(string.splitlines()) # get rid of crazy unicode linebreaks
    string = string.replace("\m", "\a") # old mac text files
    string = string.replace("\n", "\a")
    string = control_char_re.sub(' ', string)
    return string

space_re = re.compile('[ ]+')

def prepSqlString(string):
    """ change <<</>>> to <b>/</b>, replace unicode chars with 
    character code, because genome browser html cannot do unicode
    
    """
    global control_chars
    if string==None:
       string = ""
    string = pubStore.toUnicode(string)
    string = replaceSpecialChars(string)
    string = string.replace("\\", "\\\\") # mysql treats \ as escape char on LOAD DATA
    string = string.replace("<<<", "<B>")
    string = string.replace(">>>", "</B>")
    string = string.replace("\A", "<BR>")
    string = space_re.sub(' ', string)
    string = unicodeConvert.string_to_ncr(string)
    if len(string) > pubConf.maxColLen:
       logging.warn("Cutting column to %d chars, text: %s" % (pubConf.maxColLen, string[:200]))
       string = string[:pubConf.maxColLen]
    return string

def iterFileDataDir(textDir):
    return iterArticleDataDir(textDir, type="files")

def getAllBatchIds(outDir):
    """ parse batches.tab and return all available batchIds
    """
    batchIds = []
    for row in maxCommon.iterTsvRows(join(outDir, "batches.tab")):
        batchIds.append(row.batchId)
    logging.debug("Found batchIds %s in directory %s" % (batchIds, outDir))
    return batchIds

def parseUpdatesTab(outDir, minArticleId):
    """ parse updates.tab and find next available articleIds and 
    list of files that were processed in all updates

    returns (next free updateId, next free articleId, list of files that have been processed)

    """
    inFname = join(outDir, "updates.tab")

    if not isfile(inFname):
        logging.warn("could not find %s, this seems to be the first run in this dir" % inFname)
        return 0, minArticleId, []

    doneFiles = set()
    row = None
    for row in maxTables.TableParser(inFname).lines():
        rowFiles = row.files.split(",")
        doneFiles.update(rowFiles)

    if row==None:
        logging.warn("empty file %s, this seems to be the first run in this dir" % inFname)
        return 0, minArticleId, []
    logging.debug("Parsed updates.tab, files already done are %s" % doneFiles)
    return int(row.updateId)+1, int(row.lastArticleId)+1, doneFiles

def listAllUpdateIds(textDir):
    " return set of possible update Ids in textDir "
    inFname = join(textDir, "updates.tab")
    updateIds = set()
    if not isfile(inFname):
	logging.info("Could not find %s" % inFname)
        return None
    for row in maxTables.TableParser(inFname).lines():
        updateIds.add(row.updateId)
    return updateIds

def guessChunkSize(outDir):
    " get line count of  0_00000.articles.gz in outDir"
    fname = join(outDir, "0_00000.articles.gz")
    if not isfile(fname):
        raise Exception("%s does not exist, corrupted output directory from previous run?" % fname)
    lineCount = len(gzip.open(fname).readlines())-1
    logging.info("Guessing chunk size: Chunk size of %s is %d" % (fname, lineCount))
    return lineCount

def appendToUpdatesTxt(outDir, updateId, maxArticleId, files):
    " append a line to updates.tab in outDir, create file if necessary "
    outFname = join(outDir, "updates.tab")
    if len(files)==0:
        logging.info("Not writing any progress update, no new files")
        return

    logging.info("Writing progress to %s: updateId %d, %d files" % (outFname, updateId, len(files)))
    #logging.info("You must delete the last line of %s if the cluster job fails" % outFname)
    if not isfile(outFname):
        outFh = open(outFname, "w")
        headers = ["updateId", "lastArticleId", "time", "files"]
        outFh.write("\t".join(headers))
        outFh.write("\n")
    else:
        outFh = open(outFname, "a")

    row = [str(updateId), str(maxArticleId), time.asctime(), ",".join(files)]
    outFh.write("\t".join(row))
    outFh.write("\n")
    outFh.close()

def moveFiles(srcDir, trgDir):
    " move all files from src to target dir "
    for fname in os.listdir(srcDir):
        infname = join(srcDir, fname)
        outfname = join(trgDir, fname)
        if isdir(infname):
            continue
        if isfile(outfname):
            logging.debug("Deleting %s" % outfname)
            os.remove(outfname)
        logging.debug("moving file %s to %s" % (infname, outfname))
        shutil.move(infname, outfname)

def articleIdToDataset(articleId):
    """ uses the central namespace table to resolve an articleId to its dataset name 
    >>> articleIdToDataset(4300000011) 
    'imgt'
    >>> articleIdToDataset(2004499279)
    'elsevier'
    """
    articleId = int(articleId)
    restList = []
    for datasetName, artIdStart in pubConf.identifierStart.iteritems():
        rest = articleId - artIdStart
        if rest>0:
            restList.append( (datasetName, rest) )
    restList.sort(key=operator.itemgetter(1))
    #print restList
    return restList[0][0]

def iterChunks(datasets):
    """ given a list of datasets like ["pmc", "elsevier"], return a list of directory/chunkStems, 
    e.g. "/hive/data/inside/pubs/text/elsevier/0_0000.articles.gz". Used to prepare cluster jobs
    
    If dataset is already a valid filename, will only return the filename (for debugging)
    """
    for dataset in datasets:
        if isfile(dataset):
            yield dataset
        else:
            dirName = join(pubConf.textBaseDir, dataset)
            for fname in glob.glob(dirName+"/*.articles.gz"):
                yield fname

def addLoadedFiles(dbFname, fileNames):
    " given a sqlite db, create a table loadedFiles and add fileNames to it "
    fileNames = [(basename(x), ) for x in fileNames] # sqlite only accepts tuples, strip path
    con, cur = maxTables.openSqliteRw(dbFname)
    cur.execute("CREATE TABLE IF NOT EXISTS loadedFiles (fname TEXT PRIMARY KEY);")
    con.commit()
    sql = "INSERT INTO loadedFiles (fname) VALUES (?)"
    cur.executemany(sql, list(fileNames))
    con.commit()
    
def getUnloadedFnames(dbFname, newFnames):
    """ given a sqlite db and a list of filenames, return those that have not been loaded yet into the db 
    comparison looks only at basename of files 
    """
    con, cur = maxTables.openSqliteRo(dbFname)
    loadedFnames = []
    try:
        for row in cur.execute("SELECT fname from loadedFiles"):
            loadedFnames.append(row[0])
    except sqlite3.OperationalError:
        logging.debug("No loadedFiles table yet in %s" % dbFname)
        return newFnames
    #logging.debug("Files that have been loaded already: %s" % loadedFnames)

    # keep only filenames that haven't been loaded yet
    loadedFnames = set(loadedFnames)
    toLoadFnames = []
    for newFname in newFnames:
        if basename(newFname) not in loadedFnames:
            toLoadFnames.append(newFname)
            
    #logging.debug("Files that have not been loaded yet: %s" % toLoadFnames)
    return toLoadFnames

def loadNewTsvFilesSqlite(dbFname, tableName, tsvFnames):
    " load pubDoc files into sqlite db table, keep track of loaded file names "
    firstFname = tsvFnames[0]
    if firstFname.endswith(".gz"):
        firstFh = gzip.open(firstFname)
    else:
        firstFh = open(firstFname)
    headers = firstFh.readline().strip("\n#").split("\t")
    toLoadFnames = getUnloadedFnames(dbFname, tsvFnames)
    toLoadFnames.sort()

    if len(toLoadFnames)==0:
        logging.debug("No files to load")
    else:
        maxTables.loadTsvSqlite(dbFname, tableName, toLoadFnames, headers=headers, \
            primKey="articleId", intFields=["pmid", "pmcId"], idxFields=["pmid", "pmcId"], dropTable=False)
        addLoadedFiles(dbFname, toLoadFnames)

def getArtDbPath(datasetName):
    " return the sqlite database name with meta info of a dataset "
    dataDir = pubConf.resolveTextDir(datasetName)
    dbPath = join(dataDir, "articles.db")
    return dbPath

if __name__=="__main__":
    import doctest
    doctest.testmod()

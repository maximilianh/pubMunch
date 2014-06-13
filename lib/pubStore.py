# Classes to read and write documents to directories

# gzipfiles are stored in "chunks" of x documents, usually to get around 2000 
# chunks per dataset

# reader class gets path/xxxx and will open path/xxxx.articles.gz and path/xxxx.files.gz
# reader class yields files from these two files

# writer class writes to local filesystem, as two different textfiles:
# xxxx.files, xxxx.articles
# then gzips them and copies gzipfiles to shared cluster filesystem

import os, logging, sys, collections, time, codecs, shutil, tarfile, csv, glob, operator
import zipfile, gzip, re, random
try:
    import sqlite3
except ImportError:
    logging.warn("No sqlite3 loaded")

import pubGeneric, pubConf, maxCommon, unicodeConvert, maxTables, pubPubmed

from os.path import *

# need to increase maximum size of fields for csv module
csv.field_size_limit(50000000)

# DATA FIELD DEFINITIONS

articleFields=[
"articleId",  # internal number that identifies this article in the pubtools system
"externalId", # original string id of the article, e.g. PMC12343 or the PPI or PMID123123
"source",  # the data format of the article, "elsevier", "medline", "pmc" or "crawler"
"publisher", # the name of the publisher icon, e.g. "pnas" or "aai", for all of PMC just 'pmc'
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
"locFname", # filename of original file before to-text-conversion
"content" # the data from this file (newline => \a, tab => space, cr => space, \m ==> \a)
]

ArticleRec = collections.namedtuple("ArticleRec", articleFields)
emptyArticle = ArticleRec(*len(articleFields)*[""])

FileDataRec = collections.namedtuple("FileRecord", fileDataFields)
emptyFileData = FileDataRec(*len(fileDataFields)*[""])

def createEmptyFileDict(url=None, time=time.asctime(), mimeType=None, content=None, \
    fileType=None, desc=None, externalId=None, locFname=None):
    fileData = emptyFileData._asdict()
    if time!=None:
        fileData["time"]=time
    if url!=None:
        fileData["url"]=url
    if mimeType!=None:
        fileData["mimeType"]=mimeType
    if externalId!=None:
        fileData["externalId"]=externalId
    if content!=None:
        fileData["content"]=content
    if fileType!=None:
        fileData["fileType"]=fileType
    if desc!=None:
        fileData["desc"]=desc
    if locFname!=None:
        fileData["locFname"] = locFname
    logging.log(5, "Creating new file record, url=%s, fileType=%s, desc=%s" % (url, fileType, desc))
    return fileData

def createEmptyArticleDict(pmcId=None, source=None, externalId=None, journal=None, \
    id=None, origFile=None, authors=None, fulltextUrl=None, keywords=None, title=None, abstract=None, \
    publisher=None, pmid=None, doi=None):
    """ create a dictionary with all fields of the ArticleType """
    metaInfo = emptyArticle._asdict()
    metaInfo["time"]=time.asctime()
    if publisher!=None:
        metaInfo["publisher"]=publisher
    if doi:
        metaInfo["doi"]=doi
    if pmid:
        metaInfo["pmid"]=pmid
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

def splitTabFileOnChunkId(filename, outDir, chunkSize=None, chunkCount=None):
    """ 
    use the chunkId field of a tab-sep file as the output filename.
    if chunkSize is specified, ignore the chunkId field and make sure that each piece
    has chunkSize lines.
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
    i = 0
    for row in maxCommon.iterTsvRows(filename, encoding=None):
        if chunkSize==None and chunkCount==None:
            chunkId = row.chunkId
        elif chunkSize!=None:
            chunkId = "%05d" % (i / chunkSize)
        elif chunkCount!=None:
            chunkId = "%05d" % (i % chunkSize)
        data.setdefault(str(chunkId), []).append("\t".join(row)+"\n")
        i += 1

    # write to outDir
    logging.info("Splitting file data, Writing to %d files in %s/xxxx.tgz" % (len(data), outDir))
    pm = maxCommon.ProgressMeter(len(data))
    for chunkIdString, lines in data.iteritems():
        outfname = os.path.join(outDir, chunkIdString)
        logging.debug("Writing to %s" % outfname)
        fh = open(outfname, "w")
        fh.write(headerLine)
        for line in lines:
            fh.write(line)
        fh.close()
        pm.taskCompleted()

    return data.keys()

def toUnicode(var):
    " force variable to unicode, by decoding as utf8 first, then latin1 "
    if isinstance(var, unicode):
        return var
    elif type(var)==type(1):
        var = unicode(var)
    elif var==None:
        var = "NotSpecified"
    elif isinstance(var, str):
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
    if dict==None:
        return None
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

def removeTabNl(var):
    " remove tab and all forms (!) of newline characters from string "
    # RAHHH! CRAZY UNICODE LINEBREAKS
    # the following would not work because of python's interpretation of unicode
    #newDict[key] = val.replace("\t", " ").replace("\n", " ")
    # so we do this
    cleanString = " ".join(unicode(var).splitlines()).replace("\t", " ")
    #logging.debug("cleaned string is %s" % repr(newStr))
    return cleanString

def articleDictToTuple(artDict):
    " convert a dict to an article tuple "
    for key in articleFields:
        if key not in artDict:
            artDict[key] = ""
    artTuple = ArticleRec(**artDict)
    return artTuple

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
    We only copy files over if any files data was actually written 
    We only copy articles over if any articles data was actually written 

    """
    def __init__(self, fileDataFilename):
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
        self.fileFh = codecs.open(fileFname, "w", encoding="utf8")
        self.fileFh.write("#"+"\t".join(fileDataFields)+"\n")

        articleFname = os.path.join(tempDir, articleBaseName)
        self.articleFh = codecs.open(articleFname, "w", encoding="utf8") 
        self.articleFh.write("#"+"\t".join(articleFields)+"\n")

        self.outFilename = os.path.join(outDir, fileDataBasename)

    def _removeSpecChar(self, lineDict):
        " remove tab and NL chars from values of dict "
        newDict = {}
        for key, val in lineDict.iteritems():
            newDict[key] = removeTabNl(val)
        return newDict
        
    def writeFile(self, articleId, fileId, fileDict, externalId=""):
        """ appends id and data to current .file table,
            will not write if maximum filesize exceeded
        """
        if fileDict["content"]==None:
            logging.warn("file %s, object or content is None" % fileId)
            fileDict["content"] = ""

        # checked in toAscii() now
        #if len(fileDict["content"]) > pubConf.MAXTXTFILESIZE:
            #logging.info("truncating file %s, too big" % fileId)
            #fileDict["content"] = fileDict["content"][:pubConf.MAXTXTFILESIZE]

        if "externalId" not in fileDict:
            fileDict["externalId"] = fileDict["articleId"]
        if "locFname" not in fileDict:
            fileDict["locFname"] = ""

        if len(fileDict)!=len(fileDataFields):
            logging.error("column counts between file dict and file objects don't match")
            dictFields = fileDict.keys()
            dictFields.sort()
            logging.error("columns are          %s" % str(dictFields))
            expFields = fileDataFields
            expFields.sort()
            logging.error("expected columns are %s" % str(expFields))
            raise Exception()

        if "externalId" not in fileDict:
            fileDict["externalId"] = externalId
        fileDict["fileId"]=str(fileId)
        fileDict["articleId"]=str(articleId)
        # convert dict to line and write to xxxx.file 
        fileTuple = FileDataRec(**fileDict)
        fileTuple = listToUtf8Escape(fileTuple)
        line = "\t".join(fileTuple)

        #logging.log(5, "Writing line to file table, dict is %s" % fileDict)
        self.filesWritten += 1
        self.fileFh.write(line+"\n")
        
    def writeArticle(self, articleId, articleDict):
        """ appends data to current chunk """
        articleDict["articleId"]=articleId
        articleDict = self._removeSpecChar(articleDict)
        logging.log(5, "appending article info to %s: %s" % (self.articleFh.name, str(articleDict)))
        if len(articleDict)!=len(articleFields):
            logging.error("column counts between article dict and article objects don't match")
            dictFields = articleDict.keys()
            dictFields.sort()
            logging.error("columns are          %s" % str(dictFields))
            expFields = articleFields
            expFields.sort()
            logging.error("expected columns are %s" % str(expFields))
            raise("Error")

        articleTuple = ArticleRec(**articleDict)

        # convert all fields to utf8 string, remove \n and \t
        articleTuple = listToUtf8Escape(articleTuple)

        line = "\t".join(articleTuple)
        self.articleFh.write(line+"\n")
        self.articlesWritten += 1
        logging.log(5, "%d articles written" % self.articlesWritten)
        
    def _gzipAndMove(self, fname, finalName):
        " gzip fname and move to finalName "
        gzName = fname+".gz"
        if isfile(gzName):
            os.remove(gzName)
        maxCommon.runCommand("gzip %s" % fname)
        logging.debug("compressing and copying files table to %s" % finalName)
        shutil.copyfile(gzName, finalName)
        os.remove(gzName)

    def close(self, keepEmpty=False):
        """ 
        close the 3 files, copy them over to final targets and  delete the
        temps 
        """ 
        logging.debug("Copying local tempfiles over to files on server %s" % self.finalArticleName)
        assert(self.fileFh.name.endswith(".files"))

        self.fileFh.close()
        if self.articlesWritten==0:
            logging.warn("No articles received, not writing anything, but a 0 sized file for parasol")
            # just create a 0-size file for parasol
            open(self.finalArticleName, "w")
            
        if self.filesWritten > 0 or keepEmpty:
            self._gzipAndMove(self.fileFh.name, self.finalFileDataName)

        self.articleFh.close()
        if self.articlesWritten > 0 or keepEmpty:
            self._gzipAndMove(self.articleFh.name, self.finalArticleName)

def createPseudoFile(articleData):
    """ create a file from the abstract and title of an article,
    for articles that don't have fulltext (pubmed) """
    logging.debug("no file data, creating pseudo-file from abstract")
    fileData = createEmptyFileDict()
    fileData["url"] = articleData.fulltextUrl
    fileData["content"] = " ".join([" ",articleData.title, articleData.abstract, " "]) 
    fileData["mimeType"] = "text/plain"
    fileData["fileId"] = int(articleData.articleId) * (10**pubConf.FILEDIGITS)
    fileTuple = FileDataRec(**fileData)
    return fileTuple

class PubReaderFile:
    """ 
    read articles from compressed tab-sep files
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
        if isfile(fileFn):
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

        for fileData in self.fileRows:
           logging.log(5, "Read file data %s for article %s" % \
               (str(fileData.fileId), fileData.articleId))
           text = pubGeneric.forceToUnicode(fileData.content)
           fileData = fileData._replace(content=text)
           if articleId==fileData.articleId:
               logging.log(5, "adding file data")
               fileDataList.append(fileData)
           else:
               fileIds = list(set([str(x.fileId)[:pubConf.ARTICLEDIGITS] for x in fileDataList]))
               logging.log(5, "article change. yielding: articleId %s, %d files with ids %s" % \
                   (articleId, len(fileDataList), fileIds))
               assert(len(fileIds)==1)
               assert(fileIds[0]==str(articleId))
               return fileDataList, fileData
        return fileDataList, None

    def _keepOnlyMain(self, files):
        " remove all suppl files "
        mainFiles = {}
        newFiles = []
        for fileData in files:
            if fileData.fileType=="main" or fileData.fileType=="":
                newFiles.append(fileData)
        logging.log(5, "Main-text filter: got %d files, returned %d files" % (len(files), len(newFiles)))
        return newFiles

    def _keepBestMain(self, files):
        " if there is a PDF and XML or HTML version for the main text, remove the PDF. keep all suppl files "
        mainFiles = {}
        newFiles = []
        for fileData in files:
            if fileData.fileType=="main" or fileData.fileType=="":
                # we should never have two main files with same type
                assert(fileData.mimeType not in mainFiles)
                mainFiles[fileData.mimeType] = fileData
            else:
                newFiles.append(fileData)

        # this should happen only very very rarely, if main file was corrupted
        if len(mainFiles)==0:
            logging.error("No main file for article?")
            logging.error("%s" % files)
            return newFiles

        # now remove the pdf if there are better files
        if len(mainFiles)>1 and \
                "application/pdf" in mainFiles and \
                ("text/xml" in mainFiles or "text/html" in mainFiles):
            logging.debug("Removing pdf")
            del mainFiles["application/pdf"]

        # paranoia check: make sure that we still have left one file
        if not len(mainFiles)>=1:
            logging.error("no main file anymore: input %s output %s " % (files, mainFiles))
            assert(len(mainFiles)>=1)

        newFiles.insert(0, mainFiles.values()[0])
        return newFiles

    def iterArticlesFileList(self, onlyMeta=False, onlyBestMain=False, onlyMain=False):
        """ iterate over articles AND files, as far as possible

        for input files with article and file data:
            yield a tuple (articleData, list of fileData) per article 
        for input files with no article data, yield a tuple (None, [fileData])
        for input files with no file data, generate pseudo-file from abstract (title+abstract+authors)
        if onlyMeta is True, do not read .files.gz and yield (articleData, pseudoFile) 
        if onlyBestMain is True, ignore the PDF file if there are PDF and XML/Html main files
        """
        fileDataList = []
        lastFileData = None
        for articleData in self.articleRows:
            if len(fileDataList)!=0 and fileDataList[0].articleId!=articleData.articleId:
                logging.warn("skipping %s, seems that articleId is out of sync with files" %
                    articleData.articleId)
                continue
            logging.log(5, "Read article meta info for %s" % str(articleData.articleId))

            if self.fileRows!=None and not onlyMeta==True:
                # if file data is there and we want it, read as much as we can
                fileDataList, lastFileData = self._readFilesForArticle(articleData.articleId, fileDataList)
                if onlyBestMain:
                    fileDataList = self._keepBestMain(fileDataList)
                if onlyMain:
                    fileDataList = self._keepOnlyMain(fileDataList)
                yield articleData, fileDataList
                fileDataList = [lastFileData]
            else:
                # if only abstract: create pseudo file (medline)
                fileTuple = createPseudoFile(articleData)
                yield articleData, [fileTuple]

        if len(fileDataList)!=0 and lastFileData!=None:
            logging.log(5, "last line: yielding last article + rest of fileDataList")
            yield articleData, fileDataList

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

#class PubReaderPmidDir:
#    """ reads files from a directory of text files in format <pmid>.txt"""
#    def __init__(self, dirName):
#        self.dirName = dirName
#
#    def iterArticlesFileList(self, onlyMeta=False, onlyBestMain=False, onlyMain=False):
#        class ArtData:
#            def _replace(self, content=None):
#                return self
#
#            def _asdict(self):
#                return {"pmid":10, "externalId":"extId000", "articleId":100000000}
#
#
#        for fname in glob.glob(join(self.dirName, "*.txt")):
#            pmid = splitext(basename(fname))[0]
#            art = ArtData()
#            art.articleId = pmid
#            art.pmid = pmid
#            art.externalId = 
#            art.printIssn = "1234-1234"
#
#            fileObj = C()
#            fileObj.fileId = "1001"
#            fileObj.content = self.text
#            fileObj.fileType = "main"
#        
#        yield art, [fileObj]

class PubReaderTest:
    """ reads only a single text file """
    def __init__(self, fname, text=None):
        if text:
            self.text=text
        else:
            self.text = open(fname).read()

    def iterArticlesFileList(self, onlyMeta=False, onlyBestMain=False, onlyMain=False):
        class C:
            def _replace(self, content=None):
                return self

            def _asdict(self):
                return {"pmid":10, "externalId":"extId000", "articleId":100000000}


        art = C()
        art.articleId = "1000000000"
        art.pmid = "10"
        art.externalId = "extId000"
        art.printIssn = "1234-1234"
        art.url =  "http://sgi.com"

        fileObj = C()
        fileObj.fileId = "1001"
        fileObj.content = self.text
        fileObj.fileType = "main"
        fileObj.externalId = "file0000"
        fileObj.desc="desc"
        
        yield art, [fileObj]

def iterArticleDirList(textDir, onlyMeta=False, preferPdf=False, onlyMain=False):
    " iterate over all files with article/fileData in textDir "
    logging.debug("Getting filenames in dir %s" % textDir)
    fileNames = glob.glob(os.path.join(textDir, "*.articles.gz"))
    logging.debug("Found %d files in input dir %s" % (len(fileNames), textDir))
    pm = maxCommon.ProgressMeter(len(fileNames))
    for textCount, textFname in enumerate(fileNames):
        reader = PubReaderFile(textFname)
        logging.debug("Reading %s, %d files left" % (textFname, len(fileNames)-textCount))
        pr = PubReaderFile(textFname)
        artIter = pr.iterArticlesFileList(onlyBestMain=preferPdf, onlyMain=False, onlyMeta=onlyMeta)
        for article, fileList in artIter:
            yield article, fileList
        pm.taskCompleted()

def iterArticleDataDirs(textDirs, type="articles", filterFname=None, updateIds=None):
    logging.info("Getting rows from %s files in dirs: %s" % (type, textDirs))
    for textDir in textDirs:
        for row in iterArticleDataDir(textDir, type, filterFname, updateIds):
            yield row

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
        fileMask = os.path.join(textDir, baseMask)
        fileNames = glob.glob(fileMask)
        logging.debug("Looking for all fulltext files in %s, found %d files" % \
            (fileMask, len(fileNames)))
        if updateIds!=None and len(updateIds)!=0:
            logging.debug("Restricting fulltext files to updateIds %s" % str(updateIds))
            filteredFiles = []
            for updateId in updateIds:
                for fname in fileNames:
                    if basename(fname).startswith(str(updateId)+"_"):
                        filteredFiles.append(fname)
                logging.debug("Update Id %s, %d files" % (str(updateId), len(filteredFiles)))
            fileNames = list(filteredFiles)

        logging.debug("Found %d files in input dir %s" % (len(fileNames), textDir))

    pm = maxCommon.ProgressMeter(len(fileNames), stepCount=100)
    for textFname in fileNames:
        if filterFname!=None and not filterFname in textFname:
            logging.warn("Skipping %s, because file filter is set" % textFname)
            continue
        reader = PubReaderFile(textFname)
        logging.debug("Reading %s, %d files left" % (textFname, len(fileNames)-fcount))
        fcount+=1
        if type=="articles":
            for articleData in reader.articleRows:
                if "publisher" not in articleData._fields: # XX temporary bugfix as I have some old files
                    articleData = list(articleData)
                    articleData.insert(2, "")
                    articleData[3] = ""
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
    if string==None:
        return ""
    string = string.replace(u'\u2028', '\n') # one of the crazy unicode linebreaks
    string = "\n".join(string.splitlines()) # get rid of other crazy unicode linebreaks
    string = string.replace("\m", "\a") # old mac text files
    string = string.replace("\n", "\a")
    string = control_char_re.sub(' ', string)
    return string

space_re = re.compile('[ ]+')

def prepSqlString(string, maxLen=pubConf.maxColLen):
    """ change <<</>>> to <b>/</b>, replace unicode chars with 
    character code, because genome browser html cannot do unicode
    
    """
    global control_chars
    if string==None:
       string = ""
    string = toUnicode(string)
    string = replaceSpecialChars(string)
    string = string.replace("\\", "\\\\") # mysql treats \ as escape char on LOAD DATA
    string = string.replace("<<<", "<B>")
    string = string.replace(">>>", "</B>")
    string = string.replace("\A", "<BR>")
    string = space_re.sub(' ', string)
    string = unicodeConvert.string_to_ncr(string)
    if len(string) > maxLen:
       logging.warn("Cutting column to %d chars, text: %s" % (maxLen, string[:200]))
       string = string[:maxLen]
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
    logging.debug("Parsed updates.tab, %d files already done" % (len(doneFiles)))
    return int(row.updateId)+1, int(row.lastArticleId)+1, doneFiles

def listAllUpdateIds(textDir):
    " return set of possible update Ids in textDir "
    logging.info("Getting list of available update ids in text directory")
    inFname = join(textDir, "updates.tab")
    updateIds = set()
    if not isfile(inFname):
	logging.info("Could not find %s, using filenames to get updates" % inFname)
        inNames = glob.glob(join(textDir, "*.articles.gz"))
        updateIds = set([basename(x).split("_")[0] for x in inNames])
	logging.info("Found text update ids from filenames: %s" % updateIds)
        return updateIds

    for row in maxTables.TableParser(inFname).lines():
        updateIds.add(row.updateId)
    logging.info("Text update ids in %s: %s" % (inFname, updateIds))
    return updateIds

def guessChunkSize(outDir):
    " get line count of  0_00000.articles.gz in outDir"
    fname = join(outDir, "0_00000.articles.gz")
    if not isfile(fname):
        #raise Exception("%s does not exist, corrupted output directory from previous run?" % fname)
        return None
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
    con, cur = maxTables.openSqlite(dbFname, lockDb=True)
    cur.execute("CREATE TABLE IF NOT EXISTS loadedFiles (fname TEXT PRIMARY KEY);")
    con.commit()
    sql = "INSERT INTO loadedFiles (fname) VALUES (?)"
    cur.executemany(sql, list(fileNames))
    con.commit()
    
def getUnloadedFnames(dbFname, newFnames):
    """ given a sqlite db and a list of filenames, return those that have not been loaded yet into the db 
    comparison looks only at basename of files 
    """
    con, cur = maxTables.openSqlite(dbFname)
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

def sortPubFnames(fnames):
    """ 
    sort names like 0_00000, 11_1111.articles.gz in the right order from 0_00000 to 1111_11111 
    >>> sortPubFnames(["/hive/data/test/11_0000.articles.gz", "1_0000.articles.gz"])
    ['1_0000.articles.gz', '/hive/data/test/11_0000.articles.gz']
    """
    newList = []
    for fname in fnames:
        base = basename(fname).split(".")[0]
        update, chunk = base.split("_")
        update, chunk = int(update), int(chunk)
        sortKey = update*10000 + chunk
        newList.append((sortKey, fname))

    newList.sort(key=operator.itemgetter(0))
    newFnames = [el[1] for el in newList]
    return newFnames
        

def loadNewTsvFilesSqlite(dbFname, tableName, tsvFnames):
    " load pubDoc files into sqlite db table, keep track of loaded file names "
    if len(tsvFnames)==0:
        return
    logging.debug("Loading %d files into table %s, db %s" %(len(tsvFnames), tableName, dbFname))
    firstFname = tsvFnames[0]
    if firstFname.endswith(".gz"):
        firstFh = gzip.open(firstFname)
    else:
        firstFh = open(firstFname)
    #headers = firstFh.readline().strip("\n#").split("\t")
    headers = articleFields
    logging.debug("DB fields are: %s" % headers)
    toLoadFnames = getUnloadedFnames(dbFname, tsvFnames)
    toLoadFnames = sortPubFnames(toLoadFnames)

    #if not isfile(dbFname):
        #lockDb = True
    #else:
        #lockDb = False

    if len(toLoadFnames)==0:
        logging.debug("No files to load")
    else:
        indexedFields = ["pmid", "pmcId","printIssn", "eIssn", "year", "doi", "extId"]
        intFields = ["pmid", "pmcId","year"]
        maxTables.loadTsvSqlite(dbFname, tableName, toLoadFnames, headers=headers, \
            primKey="articleId", intFields=intFields, idxFields=indexedFields, dropTable=False)
        addLoadedFiles(dbFname, toLoadFnames)

datasetRanges = None

def setupDatasetRanges():
    global datasetRanges
    if datasetRanges!=None:
        return

    datasetStarts = pubConf.identifierStart.items()
    datasetStarts.sort(key=operator.itemgetter(1))
    assert(datasetStarts[-1][0]=="free")

    datasetRanges = []
    for i in range(0, len(datasetStarts)-1):
        d = datasetStarts[i]
        d2 = datasetStarts[i+1]
        datasetRanges.append((d[0], d[1], d2[1]))

def artIdToDatasetName(artId):
    """ resolve article Id to dataset name 
    >>> artIdToDatasetName(1000000000)
    'pmc'
    >>> artIdToDatasetName(4500000005)
    'yif'
    """
    artId = int(artId)
    setupDatasetRanges()
    for dataset, minId, maxId in datasetRanges:
        if artId >= minId and artId < maxId:
            return dataset

def getArtDbPath(datasetName):
    """ return the sqlite database name with meta info of a dataset """
    dataDir = pubConf.resolveTextDir(datasetName)
    dbPath = join(dataDir, "articles.db")
    return dbPath

conCache = {}
def openArticleDb(datasetName):
    if datasetName in conCache:
        con, cur = conCache[datasetName]
    else:
        path = getArtDbPath(datasetName)
        if not isfile(path):
            #return None, None
            raise Exception("Could not find %s" % path)
        logging.debug("Opening db %s" % path)
        con, cur = maxTables.openSqlite(path, asDict=True)
        conCache[datasetName] = (con,cur)
    return con, cur

def lookupArticleByArtId(artId):
    " convenience method to get article info given article Id, caches db connections "
    dataset = artIdToDatasetName(artId)
    con, cur = openArticleDb(dataset)
    return lookupArticle(con, cur, "articleId", artId)

connCache = {}

def lookupArticleByPmid(datasets, pmid):
    """ convenience method to get article info given pubmed Id, caches db connections.
        Uses eutils of local medline is not available
    """
    for dataset in datasets:
        # keep cache of db connections
        if not dataset in connCache:
            con, cur = openArticleDb(dataset)
            connCache[dataset] = con, cur
        else:
            con, cur = connCache[dataset]

        if con!=None:
            # we a local medline, lookup article locally
            art = lookupArticle(con, cur, "pmid", pmid)
        else:
            # we don't have a local medline copy, use eutils
            art = pubPubmed.getOnePmid(pmid)
        if art!=None:
            return art
    return None

def lookupArticle(con, cur, column, val):
    " uses sqlite db, returns a dict with info we have locally about article, None if not found "
    rows = None
    tryCount = 60

    while rows==None and tryCount>0:
        try:
            rows = list(cur.execute("SELECT * from articles where %s=?" % column, (val, )))
        except sqlite3.OperationalError:
            logging.info("Database is locked, waiting for 60 secs")
            time.sleep(60)
            tryCount -= 1

    if rows == None:
        raise Exception("database was locked for more than 60 minutes")
        
    if len(rows)==0:
        logging.debug("No info in local db for %s %s" % (column, val))
        return None
    # the last entry should be the newest one
    lastRow = rows[-1]

    # convert sqlite object to normal dict with strings
    result = {}
    for key, val in zip(lastRow.keys(), lastRow):
        result[key] = unicode(val)
        #if isinstance(val, basestring):
            #print repr(val)
            #result[key] = val.decode("utf8")
        #else:
            #result[key] = val

    return result

def lookupArticleData(articleId, lookupKey="articleId"):
    " lookup article meta data for an article via a database "
    #conn = maxTables.hgSqlConnect(pubConf.mysqlDb, charset="utf8", use_unicode=True)
    #sql = "SELECT * from %s where articleId=%s" % (dataset, articleId)
    #rows = maxTables.sqlGetRows(conn,sql) 
    if lookupKey=="pmid":
        dataset = "medline"
    elif lookupKey=="articleId":
        dataset = articleIdToDataset(articleId)
    assert(dataset!=None)
    textDir = join(pubConf.textBaseDir, dataset)

    if textDir not in conCache:
        dbPath = join(textDir, "articles.db")
        cur, con = maxTables.openSqlite(dbPath, asDict=True)
        conCache[textDir] = (cur, con)
    else:
        cur, con = conCache[textDir]
        
    sql = "SELECT * from articles where %s=%s" % (lookupKey, articleId)
    rows = list(cur.execute(sql))
    #assert(len(rows)==1)
    if len(rows)==0:
        #raise Exception("Could not find article %s in textDir %s" % (articleId, textDir))
        logging.error("Could not find article %s in textDir %s" % (articleId, textDir))
        return None
    articleData = rows[0]
    #authors = row["authors"]
    #author = author.split(",")[0]+" et al., "+row["journal"]
    #title = row["title"]
    #year = row["year"]
    #journal = row["journal"]
    #title = title.encode("latin1").decode("utf8")
    #text = '<small>%s (%s)</small><br><a href="%s">%s</a>' % (author, dataset, row["fulltextUrl"], title)
    return articleData

if __name__=="__main__":
    import doctest
    doctest.testmod()

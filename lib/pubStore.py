# Classes to read and write documents to directories

# gzipfiles are stored in "chunks" of x documents, usually to get around 2000 
# chunks per dataset

# reader class gets path/xxxx and will open path/xxxx.articles.gz and path/xxxx.files.gz
# reader class yields files from these two files

# writer class writes to local filesystem, as two different textfiles:
# xxxx.files, xxxx.articles
# then gzips them and copies gzipfiles to shared cluster filesystem

import os, logging, sys, collections, time, codecs, shutil, tarfile, csv, glob, operator, types
import zipfile, gzip, re, random, tempfile, copy, string
try:
    import sqlite3
except ImportError:
    logging.warn("No sqlite3 loaded")

import pubGeneric, pubConf, maxCommon, unicodeConvert, maxTables, pubPubmed

try:
    import Bio.bgzf
    bgzfLoaded = True
except ImportError:
    logging.warn("biopython not installed (use: pip install biopython, apt-get install python-biopython, yum install python-biopython")
    bgzfLoaded = False

from os.path import *
from collections import namedtuple

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
"time",     # entry creation time (conversion time)
"offset",   # offset in .files, number of bytes (NOT number of unicode characters).
"size"      # total size (in bytes, not utf8 characters) of all files in this article + size of abstract
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


refArtFields = ["articleId", "externalId", "artDoi", "artPmid"]
refFields    = ["authors", "title", "journal", "year", "month", "vol", "issue", "page", "pmid", "doi"]

ArticleRec = namedtuple("ArticleRec", articleFields)
emptyArticle = ArticleRec(*len(articleFields)*[""])

FileDataRec = namedtuple("FileRecord", fileDataFields)
emptyFileData = FileDataRec(*len(fileDataFields)*[""])

RefRec = namedtuple("citRec", refFields)

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
    """ 
    if string is not already a unicode strin (can happen due to upstream programming error):
    force variable to a unicode string, by decoding from utf8 first, then latin1 """
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
        var = var.encode("utf8")
        utf8List.append(var)
    return utf8List

def dictToUtf8Escape(dict):
    """ convert dict of variables to utf8 string as well as possible and
    replace \n and \t
    """ 
    if dict==None:
        return None
    utf8Dict={}
    for key, var in dict.iteritems():
        var = toUnicode(var)
        var = replaceSpecialChars(var)
        utf8Dict[key]=var
    return utf8Dict

def removeTabNl(var):
    " remove tab and all forms (!) of newline characters from string "
    # RAHHH! CRAZY UNICODE LINEBREAKS
    # the following would not work because of python's interpretation of unicode
    #newDict[key] = val.replace("\t", " ").replace("\n", " ")
    # there are more newlines than just \n and \m when one is using the 
    # 'for line in file' construct in python
    # so we do this
    if type(var)==types.IntType:
        return str(var)
    cleanString = " ".join(var.splitlines()).replace("\t", " ")
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
        <chunkId>.files    = raw file content and some meta (filetype, url)
        <chunkId>.articles = article meta data (author, year, etc)

    The constructor takes only the .files or .articles base filename as input and will
    derive outDir and chunkId from it.

    all writes will first go to tempDir, and will only be copied over 
    to outDir on .close()
    We only copy files over if any files data was actually written 
    We only copy articles over if any articles data was actually written 

    """
    def __init__(self, fileDataFilename):
        self.articlesWritten = 0
        self.filesWritten = 0
        self.tempDir = pubConf.getTempDir()

        # convoluted way to find output filenames
        # needed because of parasol 
        outDir = os.path.dirname(fileDataFilename)
        fileDataBasename = os.path.basename(fileDataFilename)
        chunkId = fileDataBasename.split(".")[0]

        ext = ".gz" # use .gz extension even if not compressing, saves a ton of file name problems
        if pubConf.compress:
            if bgzfLoaded:
                openFunc = Bio.bgzf.open
            else:
                openFunc = gzip.open
        else:
            openFunc = open

        self.fileBaseName = chunkId+".files"+ext
        articleBaseName = chunkId+".articles"+ext
        refBaseName = chunkId+".refs"

        # setup reference table handle
        self.refDir = join(outDir, "refs")
        self.tempRefName = join(self.tempDir, refBaseName)
        self.refFh = None

        self.finalArticleName = join(outDir, articleBaseName)
        self.finalFileDataName    = join(outDir, self.fileBaseName)
        self.finalRefFname = join(self.refDir, refBaseName)

        # setup file and article table handles
        # in temporary directory, so we do not leave behind half-written
        # chunks. Temp files are moved over to final on self.close()
        #self.tmpFileFname = os.path.join(self.tempDir, self.fileBaseName)
        self.tmpFileFname = tempfile.mktemp(prefix="pubStore.files.")
        self.fileFh = openFunc(self.tmpFileFname, "w")
        self.fileFh.write("#"+"\t".join(fileDataFields)+"\n")

        #self.tmpArticleFname = os.path.join(self.tempDir, articleBaseName)
        self.tmpArticleFname = tempfile.mktemp(prefix="pubStore.article.")
        self.articleFh = openFunc(self.tmpArticleFname, "w")
        self.articleFh.write("#"+"\t".join(articleFields)+"\n")

        maxCommon.delOnExit(self.tmpFileFname)
        maxCommon.delOnExit(self.tmpArticleFname)

        self.outFilename = os.path.join(outDir, fileDataBasename)
        logging.debug("pubStore writer open. tmp files %s and %s. Dest files %s and %s" % \
            (self.tmpArticleFname, self.tmpFileFname, self.finalArticleName, self.finalFileDataName))

    def _removeSpecChar(self, lineDict):
        " remove tab and NL chars from values of dict "
        newDict = {}
        for key, val in lineDict.iteritems():
            newDict[key] = removeTabNl(val)
        return newDict
        
    def writeRefs(self, artDict, refRows):
        " write references to table in refs/ subdir"
        if self.refFh==None:
            # lazily open ref file, add headers
            if not os.path.isdir(self.refDir):
                try:
                    os.makedirs(self.refDir)
                except OSError:
                    logging.info("makedir %s failed, probably just race condition" % self.refDir)
                    pass
            self.refFh = open(self.tempRefName, "w")
            logging.info("Created tempfile for refs %s" % self.refFh.name)
            maxCommon.delOnExit(self.tempRefName)

            refHeaders = copy.copy(refArtFields)
            refHeaders.extend(refFields)
            self.refFh.write("#"+"\t".join(refHeaders)+"\n")

        # prepare a list of article IDs of the source article
        srcArtFields = []
        for artField in refArtFields:
            if artField=="artDoi":
                artField="doi"
            if artField=="artPmid":
                artField="pmid"
            artVal = artDict[artField]
            srcArtFields.append(artVal)
        srcPrefix = "\t".join(srcArtFields)+"\t"

        # output all references
        logging.debug("Writing %d references for article %s" % (len(refRows), artDict["externalId"]))
        for ref in refRows:
            # output the source article IDs
            self.refFh.write(srcPrefix.encode("utf8"))

            # output the reference article fields
            self.refFh.write(u'\t'.join(ref).encode("utf8"))
            self.refFh.write("\n")

    def writeFile(self, articleId, fileId, fileDict, externalId=""):
        """ appends id and data to current .file table,
        """
        assert("content" in fileDict)

        if fileDict["content"]==None:
            logging.warn("file %s, object or content is None" % fileId)
            fileDict["content"] = ""

        if "externalId" not in fileDict:
            fileDict["externalId"] = fileDict["articleId"]

        self._checkFields(fileDict, fileDataFields)

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
        
    def _checkFields(self, inDict, fieldList):
        """ makes sure that inDict contains one key for each string in fieldList.
        Keys not found are set to empty strings.
        Aborts if a key is found in inDict that is not in fieldList.
        """
        gotFields = set(inDict.keys())
        mustHaveFields = set(fieldList)
        unknownFields = gotFields - mustHaveFields
        if len(unknownFields)>0:
            raise Exception("article/file writer got unknown fields: %s" % repr(unknownFields))

        missingFields = mustHaveFields - gotFields
        logging.log(5, "PubStore Writer: Adding empty strings for: %s" % missingFields)
        for key in missingFields:
            inDict[key] = ""
        return inDict


    def writeArticle(self, articleId, articleDict):
        """ appends data to current chunk. article info has to be written before the files, 
        otherwise the offset in the article dict will be wrong. 
        """
        logging.log(5, "appending article info to %s: %s" % (self.tmpArticleFname, str(articleDict)))
        articleDict["articleId"]=articleId

        # fill the "offset" field
        filePos = 0
        if self.fileFh is not None:
            filePos = self.fileFh.tell()
        articleDict["offset"] = str(filePos)

        articleDict = self._removeSpecChar(articleDict)
        articleDict = self._checkFields(articleDict, articleFields)

        articleTuple = ArticleRec(**articleDict)

        # convert all fields to utf8 string, remove \n and \t
        articleTuple = listToUtf8Escape(articleTuple)

        line = "\t".join(articleTuple)
        self.articleFh.write(line+"\n")
        self.articlesWritten += 1
        logging.log(5, "%d articles written" % self.articlesWritten)
        
    def writeDocs(self, artDict, fileDicts):
        " write all article and files in one go. Optionally extract images from PDFs. "
        # set the 'size' field of the article
        totalSize = 0
        for fileDict in fileDicts:
            totalSize += len(fileDict['content'])
        totalSize += len(artDict['abstract'])
        artDict['size'] = str(totalSize)

        self.writeArticle(artDict['articleId'], artDict)

        for fileDict in fileDicts:
            self.writeFile(artDict['articleId'], fileDict['fileId'], fileDict)


    def close(self, keepEmpty=False):
        """ 
        close the 3 files, move them over to final targets 
        """ 
        logging.debug("Moving local tempfiles over to files on server %s" % self.finalArticleName)

        self.fileFh.close()
        self.articleFh.close()

        if self.articlesWritten==0:
            logging.warn("No articles received, not writing anything, but creating a 0 sized file for parasol")
            # just create a 0-size file for parasol
            open(self.finalArticleName, "w")
            
        if self.filesWritten > 0 or keepEmpty:
            logging.debug("moving articles table to %s" % self.finalArticleName)
            shutil.move(self.tmpFileFname, self.finalFileDataName)

        if self.articlesWritten > 0 or keepEmpty:
            logging.debug("moving files table to %s" % self.finalFileDataName)
            shutil.move(self.tmpArticleFname, self.finalArticleName)

        if self.refFh!=None:
            self.refFh.close()
            shutil.move(self.refFh.name, self.finalRefFname)

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
    read articles from tab-sep files
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
        if isfile(articleFn) and getsize(articleFn)!=0:
            self.articleRows = maxCommon.iterTsvRows(articleFn, encoding="utf8")
                
        self.fileRows = None
        if isfile(fileFn) and getsize(fileFn)!=0:
            self.fileRows  = maxCommon.iterTsvRows(fileFn, encoding="utf8")

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

    def _keepBestMain(self, files, preferType):
        """ if there are several main text formats, keep only one of them.
        preferType can be "pdf" or "xml" (which includes html files)
        Keep all other files 
        """
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

        if len(mainFiles)==1:
            newFiles.insert(0, mainFiles.values()[0])
            return newFiles

        # remove the pdf if there are better files
        if preferType=="xml":
            if "application/pdf" in mainFiles and \
                    ("text/xml" in mainFiles or "text/html" in mainFiles):
                logging.debug("Removing pdf")
                del mainFiles["application/pdf"]

        elif preferType=="pdf":
            # remove the xml if there are PDF files
            if "application/pdf" in mainFiles:
                if "text/xml" in mainFiles:
                    del mainFiles["text/xml"]
                    logging.debug("Removing xml")
                if "text/html" in mainFiles:
                    del mainFiles["text/html"]
                    logging.debug("Removing html")
        else:
            assert(False)

        # paranoia check: make sure that we still have left one file
        if not len(mainFiles)>=1:
            logging.error("no main file: in %s out %s " % (files, mainFiles))
            raise Exception("no main file left")

        newFiles.insert(0, mainFiles.values()[0])
        return newFiles

    def iterArticlesFileList(self, algPrefs):
        """ iterate over articles AND files, as far as possible

        for input files with article and file data:
            yield a tuple (articleData, list of fileData) per article
        for input files with no article data, yield a tuple (None, [fileData])
        for input files with no file data, generate pseudo-file from abstract
        (title+abstract+authors)

        algPrefs.onlyMeta == True: do not read .files.gz and yield
        (articleData, pseudoFile)
        algPrefs.onlyMain == True: skip supplemental files
        algPrefs.preferXml == True: run on only one main text file.
        skip the PDF file if there are PDF and XML/Html main files. 
        algPrefs.preferPdf == True: run on only one main text file.
        skip XML or HTML files if there are multiple file types.
        """
        fileDataList = []
        lastFileData = None
        if self.articleRows==None:
            raise StopIteration

        for articleData in self.articleRows:
            if len(fileDataList)!=0 and fileDataList[0].articleId!=articleData.articleId:
                logging.warn("skipping %s, seems that articleId is out of sync with files" %
                    articleData.articleId)
                continue
            logging.log(5, "Read article meta info for %s" % str(articleData.articleId))

            if self.fileRows!=None and (algPrefs is None or (algPrefs!=None and algPrefs.onlyMeta==False)):
                # if file data is there and we want it, read as much as we can
                fileDataList, lastFileData = \
                    self._readFilesForArticle(articleData.articleId, fileDataList)

                if algPrefs!=None:
                    if algPrefs.onlyMain:
                        fileDataList = self._keepOnlyMain(fileDataList)
                    if algPrefs.preferXml:
                        fileDataList = self._keepBestMain(fileDataList, "xml")
                    if algPrefs.preferPdf:
                        fileDataList = self._keepBestMain(fileDataList, "pdf")

                yield articleData, fileDataList
                fileDataList = [lastFileData]

            else:
                # if only abstract (e.g. medline): create pseudo file 
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
    """ reads a single text file or a directory of text files"""
    def __init__(self, fname, text=None):
        if fname!=None:
            self.text = None
            if isdir(fname):
                self.fnames = glob.glob(join(fname, "*.txt"))
            elif isfile(fname):
                self.fnames = [fname]
            else:
                raise Exception("Could not open %s" % fname)
        elif text!=None:
            self.text=text
        else:
            assert(False)
        self.artId = 1000000000


    def _makeArtFile(self, text, fname):
        class C:
            def _replace(self, content=None):
                return self

            def _asdict(self):
                return {"pmid":10, "externalId":"extId000", "articleId":100000000}

        art = C()
        art.articleId = str(self.artId)
        self.artId += 1
        fnameBase = basename(fname.replace("PMID","").replace("PMC","")).split(".")[0]
        if fnameBase.isdigit():
            art.pmid = fnameBase
        else:
            art.pmid = "000000"
        art.externalId = fname
        art.printIssn = "1234-1234"
        art.url =  "http://www.pubmed.com"

        fileObj = C()
        fileObj.fileId = "1000"
        fileObj.content = text
        fileObj.fileType = "main"
        fileObj.externalId = "file0000"
        fileObj.desc="desc"

        lines = text.splitlines()
        abstract = ""
        title = ""
        for line in lines:
            if line.startswith("abstract: "):
                abstract = string.split(line, ": ", 1)[1]
            if line.startswith("title: "):
                title = string.split(line, ": ", 1)[1]
        art.title = title
        art.abstract = abstract

        return art, [fileObj]

    def iterArticlesFileList(self, algPrefs):
        if self.text != None:
            yield self._makeArtFile(self.text, "1000")

        for fname in self.fnames:
            text = open(fname).read() 
            yield self._makeArtFile(text, fname)

def iterArticleDirList(textDir, algPrefs=None):
    """ iterate over all files with article/fileData in textDir.
    This yields tuples (articleDict, list of fileDicts)
    """
    logging.debug("Getting filenames in dir %s" % textDir)
    fileNames = getAllArticleFnames(textDir)
    logging.debug("Found %d files in input dir %s" % (len(fileNames), textDir))
    pm = maxCommon.ProgressMeter(len(fileNames))
    for textCount, textFname in enumerate(fileNames):
        reader = PubReaderFile(textFname)
        logging.debug("Reading %s, %d files left" % (textFname, len(fileNames)-textCount))
        pr = PubReaderFile(textFname)
        artIter = pr.iterArticlesFileList(algPrefs)
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
    assert(type in ["articles", "files"])
        
    if isfile(textDir):
        fileNames = [textDir]
        logging.debug("Found 1 file, %s" % textDir)
    else:
        fileNames = getAllArticleFnames(textDir, type=type)
        logging.debug("Looking for all fulltext files in %s, found %d files" % \
            (textDir, len(fileNames)))
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

#def iterFileDataDir(textDir):
    #return iterArticleDataDir(textDir, type="files")

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
        logging.info("could not find %s, this seems to be the first run in this dir" % inFname)
        return 0, minArticleId, []
    logging.info("Reading IDs of files that are already done from %s" % inFname)

    doneFiles = set()
    row = None
    logging.debug("Parsing %s" % inFname)
    for row in maxCommon.iterTsvRows(inFname, encoding="utf8"):
        rowFiles = row.files.split("|")
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
        fname = join(outDir, "0_00000.articles")
        if not isfile(fname):
            return None
        lineCount = len(open(fname).readlines())
    else:
        lineCount = len(gzip.open(fname).readlines())
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

    row = [str(updateId), str(maxArticleId), time.asctime(), "|".join(files)]
    outFh.write("\t".join(row))
    outFh.write("\n")
    outFh.close()

def moveFiles(srcDir, trgDir, nameList=None):
    """ move all files from src to target dir, prefix is typically sth like ["articles", "files"] """
    logging.info("Moving files from %s to %s" % (srcDir, trgDir))
    if not isdir(trgDir):
        logging.info("Creating directory %s" % trgDir)
        os.makedirs(trgDir)

    for fname in os.listdir(srcDir):
        if nameList and not basename(fname).split(".")[1] in nameList:
            logging.log(5, "Not moving %s" % fname)
            continue
        infname = join(srcDir, fname)
        outfname = join(trgDir, fname)
        #if isdir(infname) and not basename(infname) in subDirs:
            #continue
        if isdir(infname):
            continue
        if isfile(outfname):
            logging.debug("Deleting %s" % outfname)
            os.remove(outfname)
        logging.debug("moving %s to %s" % (infname, outfname))
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
            for fname in getAllArticleFnames(dirName):
                yield fname

def addLoadedFiles(con, cur, fileNames):
    " given a sqlite db, create a table loadedFiles and add fileNames to it "
    #dbFname = getArtDbPath(inDir)
    #assert(isfile(dbFname))
    #fileNames = [(basename(x), ) for x in fileNames] # sqlite only accepts tuples, strip path
    #con, cur = maxTables.openSqlite(dbFname, lockDb=True)
    fileNames = [(basename(s),) for s in fileNames]
    cur.execute("CREATE TABLE IF NOT EXISTS loadedFiles (fname TEXT PRIMARY KEY);")
    con.commit()
    logging.debug("INSERTing %d filenames into table loadedFiles" % len(fileNames))
    sql = "INSERT INTO loadedFiles (fname) VALUES (?)"
    cur.executemany(sql, fileNames)
    con.commit()
    
def getUnloadedFnames(con, cur, newFnames):
    """ given a sqlite db and a list of filenames, return those that have not
    been loaded yet into the db. Looks only at basename of files.
    """
    loadedFnames = []
    try:
        for row in cur.execute("SELECT fname from loadedFiles"):
            loadedFnames.append(basename(row[0]))
    except sqlite3.OperationalError:
        logging.debug("No loadedFiles table yet")
        return newFnames
    logging.debug("Files that have been loaded already: %s" % loadedFnames)

    # keep only filenames that haven't been loaded yet
    loadedFnames = set(loadedFnames)
    toLoadFnames = []
    for newFname in newFnames:
        if basename(newFname) not in loadedFnames:
            toLoadFnames.append(basename(newFname))
            
    logging.debug("Files that have not been loaded yet: %s" % toLoadFnames)
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
        
def chunkIdFromFname(fname):
    " given the path of chunk, return it's chunk ID, like 0_00001 "
    return basename(fname).split(".")[0]

def addToDatabase(con, cur, tsvFnames):
    " load articles files into sqlLite db table. Adds a field 'chunkId'. "
    tableName = "articles"
    allFields = list(tuple(articleFields)) # make a deep copy of list
    allFields.append("chunkId")

    idxFields = ["pmid", "pmcId","printIssn", "eIssn", "year", "doi", "extId"]
    intFields = ["pmid", "pmcId","year","offset"]
    primKey = "articleId"

    # create table
    tableFields = articleFields
    tableFields.append("chunkId")
    createSql, idxSqls = maxTables.makeTableCreateStatement(tableName, articleFields, \
        intFields=intFields, idxFields=idxFields, primKey=primKey)
    logging.log(5, "creating table with %s" % createSql)
    cur.execute(createSql)
    con.commit()

    logging.info("Loading data into table")
    tp = maxCommon.ProgressMeter(len(tsvFnames))
    sql = "INSERT INTO %s (%s) VALUES (%s)" % (tableName, ", ".join(allFields), ", ".join(["?"]*len(allFields)))
    rowCount = 0
    for tsvName in tsvFnames:
        logging.debug("Loading file %s" % tsvName)
        if os.path.getsize(tsvName)==0:
            logging.debug("Skipping %s, zero size" % tsvName)
            continue
        rows = list(maxCommon.iterTsvRows(tsvName))
        chunkId = chunkIdFromFname(tsvName)

        # add the chunkId field
        newRows = []
        for row in rows:
            newRow = list(row)
            newRow.append(chunkId)
            newRows.append(newRow)
        rowCount += len(newRows)

        logging.log(5, "Running Sql %s against %d rows" % (sql, len(newRows)))
        cur.executemany(sql, newRows)
        con.commit()
        tp.taskCompleted()

    logging.info("Adding indexes to table")
    for idxSql in idxSqls:
        cur.execute(idxSql)
        con.commit()

    addLoadedFiles(con, cur, tsvFnames)
    logging.info("Loaded %s chunks into index, %d new rows" % (len(tsvFnames), rowCount))

#def loadNewTsvFilesSqlite(inDir, tableName, tsvFnames):
    #" load pubDoc files that are not loaded yet into sqlite db table, mark them as loaded at the end "
    #if len(tsvFnames)==0:
        #return
    #logging.debug("Preparing to load %d files into table %s, db dir %s" %(len(tsvFnames), tableName, inDir))
    #firstFname = tsvFnames[0]
    #if firstFname.endswith(".gz"):
        #firstFh = gzip.open(firstFname)
    #else:
        #firstFh = open(firstFname)
    #logging.debug("fields in first input file are: %s" % )

    #con, cur = openArticleDb(textDir)
    #toLoadFnames = getUnloadedFnames(con, cur, tsvFnames)
    #logging.debug("Loading %d files" % (len(toLoadFnames)))
    #toLoadFnames = sortPubFnames(toLoadFnames)
#
    #if len(toLoadFnames)==0:
        #logging.debug("No new files to load")
    #else:
        ##maxTables.loadTsvSqlite(dbFname, tableName, toLoadFnames, headers=headers, \
            #primKey="articleId", intFields=intFields, idxFields=indexedFields, dropTable=False)
        #con, cur = openArticleDb(inDir)
        #loadIndexes(con, cur, toLoadFnames)

def updateSqlite(textDir):
    """ load all .articles files that are not currently indexed 
    into the sqlite database 
    """
    artFnames = getAllArticleFnames(textDir)
    assert(len(artFnames)!=0) # there are no input files in the text data directory
    dbPath = getArtDbPath(textDir)
    if isfile(dbPath):
        con, cur = openArticleDb(textDir)
        copyBack = False
    else:
        # if db does not exist yet, use max speed: write to ramdisk and copy back.
        ramDbPath = pubGeneric.getFastUniqueTempFname()
        logging.info("First load. Creating temporary sqlite db on ramdisk %s" % ramDbPath)
        copyBack = True
        con, cur = maxTables.openSqlite(ramDbPath, lockDb=True)
        
    artFnames = [basename(x) for x in artFnames]
    toLoadFnames = getUnloadedFnames(con, cur, artFnames)
    toLoadPaths = [join(textDir, fname) for fname in toLoadFnames]

    addToDatabase(con, cur, toLoadPaths)
    con.close()

    if copyBack:
        shutil.copy(ramDbPath, dbPath)
        os.remove(ramDbPath)

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
    dataDir = pubConf.resolveTextDir(datasetName, mustFind=True)
    dbPath = join(dataDir, "articles.db")
    return dbPath

conCache = {}

def openArticleDb(datasetName, mustOpen=False, useRamdisk=False):
    " open an article sqlite DB, return (conn, cur) tuple "
    if datasetName in conCache:
        con, cur = conCache[datasetName]
    else:
        path = getArtDbPath(datasetName)
        if path is None or not isfile(path):
            logging.debug("Creating new file %s" % path)
            if mustOpen:
                raise Exception("Could not open dataset %s" % datasetName)
            #return None, None
        if useRamdisk:
                ramDbPath = pubGeneric.getFastUniqueTempFname()
                logging.info("Copying %s to %s for faster access" % (path, ramDbPath))
                maxCommon.delOnExit(ramDbPath)
                shutil.copy(path, ramDbPath)
                path = ramDbPath

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

def lookupArticleByPmid(datasets, pmid, preferLocal=True):
    """ convenience method to get article info given pubmed Id, caches db connections.
        Uses eutils if local medline is not available and preferLocal is False
    """
    for dataset in datasets:
        con = None
        if preferLocal:
            # keep cache of db connections
            if not dataset in connCache:
                con, cur = openArticleDb(dataset)
                if con is None and preferLocal:
                    raise Exception("Could not find a local copy of Medline. Use a command line option to switch to remote NCBI Eutils lookups. For low-volume crawls (< 10000), remote lookups are sufficient.")
                connCache[dataset] = con, cur
            else:
                con, cur = connCache[dataset]

        if con is None:
            # we don't have a local database, use eutils
            art = pubPubmed.getOnePmid(pmid)
        else:
            # we have a local medline, lookup article locally
            art = lookupArticle(con, cur, "pmid", pmid)

        if art!=None:
            return art
    return None

def lookupArticle(con, cur, column, val):
    " uses sqlite db, returns a dict with info we have locally about last matching article, None if not found "
    whereExpr = "%s=%s" % (column, val)
    return list(iterArticlesWhere(con, cur, whereExpr))[-1]
    
def iterArticlesWhere(con, cur, whereExpr):
    " yields dicts for article that satisfy where expression "
    rows = None
    tryCount = 60

    while rows==None and tryCount>0:
        try:
            rows = list(cur.execute("SELECT * from articles WHERE %s" % whereExpr))
        except sqlite3.OperationalError:
            logging.info("Database is locked, waiting for 60 secs")
            time.sleep(60)
            tryCount -= 1

    if rows == None:
        raise Exception("database was locked for more than 60 minutes")
        
    if len(rows)==0:
        logging.warn("No info in local db for %s" % (whereExpr))
    # the last entry should be the newest one

    for row in rows:
        # convert sqlite object to normal dict with strings
        rowDict = {}
        for key, val in zip(row.keys(), row):
            rowDict[key] = val
        yield rowDict

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
        dbPath = getArtDbPath(textDir)
        #assert(isfile(dbPath))
        if not (isfile(dbPath)):
            return None
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

def makeChunkPath(inDir, chunkId, tabType="articles"):
    " construct path to articles table file given input dir and chunkId "
    artPath = join(inDir, "%s.%s" % (chunkId, tabType))
    if not isfile(artPath):
        artPath = join(inDir, "%s.%s.gz" % (chunkId, tabType))
    return artPath
        
def lookupFullDocs(inDirs, whereExpr):
    """
    inDirs is a list of directories with pubStore sqlite indexes (article.db files)
    query is the part behind the WHERE in an sql command on the articles table
    yields tuples of (articleRow, list of fileRow).
    """
    for inDir in inDirs:
        con, cur = openArticleDb(inDir, mustOpen=True)
        fileRows = []
        for artDict in iterArticlesWhere(con, cur, whereExpr):
            artId = artDict["articleId"]
            chunkId, offset = artDict["chunkId"], artDict["offset"]
            filesPath = makeChunkPath(inDir, chunkId, "files")
            logging.debug("Opening %s" % filesPath)
            filesFh = Bio.bgzf.open(filesPath)
            fileReader = maxCommon.TsvReader(filesFh)
            logging.debug("Seeking to offset %s" % offset)
            fileReader.seek(int(offset))

            # pull out all files with this article ID
            while True:
                row = fileReader.nextRow()
                if row is None or row.fileId[:len(artId)]!=artId:
                    break
                fileRows.append(row._asdict())
            yield artDict, fileRows
        
def getAllArticleFnames(inDir, type="articles"):
    " find all article chunks in inDir "
    inFnames = glob.glob(join(inDir, "*.%s.gz" % type))
    if len(inFnames)==0:
        inFnames = glob.glob(join(inDir, "*.%s" % type))
    return inFnames


def iterPubReaders(inDir):
    " yield a PubReader for each input chunk in inDir "
    fnames = getAllArticleFnames(inDir)
    pm = maxCommon.ProgressMeter(len(fnames))
    logging.info("found %d input chunks in %s" % (len(fnames), inDir))
    for fname in fnames:
        pr = PubReaderFile(fname)
        yield pr
        pm.taskCompleted()

def dictToMarkLines(d):
    """ convert a dict to newline-sep strings like '||key: value' and return long string """
    lines = []
    keys = d.keys()
    for key in sorted(keys):
        val = d[key]
        if type(val)==types.IntType:
            val = str(val)
        if val=="":
            continue
        if key!="content":
            lines.append("|%s: %s" % (key.encode("utf8"), val.encode('utf')))

    contStr = d.get("content")
    if contStr is not None:
        lines.append(d["content"].replace("\a", "\n").encode("utf8"))

    return "\n".join(lines)

if __name__=="__main__":
    import doctest
    doctest.testmod()

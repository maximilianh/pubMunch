from __future__ import print_function
from __future__ import division
# generic functions for all pubtools, like logging, finding files,
# ascii-conversion, section splitting etc

from builtins import range
import os, logging, tempfile, sys, re, unicodedata, subprocess, time, types, traceback, \
    glob, operator, doctest, ftplib, random, shutil, atexit, pickle
import pubConf, pubXml, maxCommon, pubStore, maxRun, maxTables, pubKeyVal
from os.path import *
from distutils.spawn import find_executable

from time import gmtime, strftime

try:
    import leveldb
except:
    pass

# global var for the current headnode as specified on command line
forceHeadnode = None

# some data for countBadChars
all_chars = (chr(i) for i in range(0x110000))
specCodes = set(range(0,32))
goodCodes = set([7,9,10,11,12,13]) # BELL, TAB, LF, NL, FF (12), CR are not counted
badCharCodes = specCodes - goodCodes
control_chars = ''.join(map(chr, badCharCodes))
control_char_re = re.compile('[%s]' % re.escape(control_chars))

def getFastUniqueTempFname():
    " create unique tempdir on ramdisk, delete on exit "
    tempFname = tempfile.mktemp(dir=pubConf.getFastTempDir())
    maxCommon.delOnExit(tempFname)
    return tempFname

class Timeout(Exception):
    pass

def openKeyValDb(dbName, newDb=False, singleProcess=False, prefer=None):
    " factory function: returns the right db object given a filename "
    #return LevelDb(dbName, newDb=newDb)
    # possible other candidates: mdb, cdb, hamsterdb
    if prefer=="server":
        return pubKeyVal.RedisDb(dbName, newDb=newDb, singleProcess=singleProcess)
    else:
        return pubKeyVal.SqliteKvDb(dbName, newDb=newDb, singleProcess=singleProcess, tmpDir=pubConf.getFastTempDir())

def createDirRace(dirPath):
    """ create a directory on a cluster node, trying to fix race conditions """
    if not os.path.isdir(dirPath):
        time.sleep(random.randint(1,3)) # make sure that we are not all trying to create it at the same time
        if not os.path.isdir(dirPath):
            try:
                os.makedirs(dirPath)
            except OSError:
                logging.debug("Ignoring OSError, directory %s seems to exist already" % dirPath)

def getFromCache(fname):
    """ Given a network path, try to find a copy of this file on the local temp disk of a cluster node.
    Return the path on the local disk. If there is no copy yet, copy it over and return the path.
    """
    locCacheDir = join(pubConf.getTempDir(), "fileCache")
    createDirRace(locCacheDir)
    locPath = join(locCacheDir, basename(fname))
    logging.debug("Getting a local cache path for %s" % fname)
    if isfile(locPath):
        return locPath
    # it doesn't exist
    #fobj, locTmpName = makeTempFile(prefix=basename(fname), suffix=".tmp")
    # copy over to local temp name, takes a while
    locTmpName = tempfile.mktemp(prefix=basename(fname), suffix=".tmp")
    time.sleep(random.randint(1,3)+random.random()) # let's add some randomness
    logging.debug("Copying %s to %s" % (fname, locTmpName))
    shutil.copy(fname, locTmpName)
    # if another process copied it over by now: roll back
    if isfile(locPath):
        #fobj.close() # = delete
        os.remove(locTmpName)
        return locPath
    # move on local node to final name
    logging.debug("Moving %s to %s" % (locTmpName, locPath))
    shutil.move(locTmpName, locPath)
    return locPath

def runCommandTimeout(command, timeout=30, bufSize=128000, env=None, shell=True):
    """
    runs command, returns after timeout, kills subprocess if it takes longer than timeout seconds
    print runCommandTimeout(["ls", "-l"])
    print runCommandTimeout(["find", "/"], timeout=3) #should timeout

    returns stdout, stderr, ret
    """
    if type(command)==list:
        logging.log(5, "running command %s" % " ".join(command))
    else:
        logging.log(5, "running command %s" % command)
    if os.name=="nt":
        closeFds = False
    else:
        closeFds = True
    proc = subprocess.Popen(command, bufsize=bufSize, stdout=subprocess.PIPE, \
        stderr=subprocess.PIPE, shell=shell, close_fds=closeFds, env=env)
    poll_seconds = 1
    deadline = time.time()+timeout
    while time.time() < deadline and proc.poll() == None:
        time.sleep(poll_seconds)

    if proc.poll() == None:
        proc.terminate()
        time.sleep(1)
        proc.kill()
        logging.error("process %s timed out" % (str(command)))
        return "", "", 1

    stdout, stderr = proc.communicate()
    return stdout, stderr, proc.returncode

def findFiles(dir, extensions):
    """ find all files in or below dir with one of several extensions
    extensions is a list, e.g. [".tab", ".txt"] or just a string like ".psl"

    returns typles: (relative directory to dir, full file path)
    """
    if isinstance(extensions, str):
        extensions = [extensions]
    logging.debug("Reading filenames in %s with extensions %s" % (dir, str(extensions)))
    result = set()
    for dirPath, dirNames, fileNames in os.walk(dir):
        for fileName in fileNames:
            for ext in extensions:
                if fileName.endswith(ext):
                    #if (dirPath==dir): ?? Why this line ??
                        #continue
                    relDir = relpath(dirPath, dir)
                    fullPath = os.path.join(dirPath, fileName )
                    if os.path.getsize(fullPath)==0:
                        continue
                    result.add( (relDir, fullPath) )
    logging.info("Found %d files" % len(result))
    return result


def setupLoggingOptions(options):
    setupLogging("", options)

def verboseFunc(message):
    " we add this to logging "
    logging.log(5, message)

def addGeneralOptions(parser, noCluster=False, logDir=False, keepTemp=False):
    """ add the options that most cmd line programs accept to optparse parser object """
    parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="show more debug messages")
    #parser.add_option("", "--quiet", dest="verbose", action="store_true", help="show more debug messages")
    if not noCluster:
        parser.add_option("-c", "--cluster", dest="cluster", action="store", help="override the default cluster head node from the config file, or 'localhost'")
    if logDir:
        parser.add_option("-l", "--logDir", dest="logDir", action="store", help="log messages to directory")
    if keepTemp:
        parser.add_option("", "--keepTemp", dest="keepTemp", action="store_true", help="keep temporary files, for debugging")
    return parser

def logToFile(logFileName):
    " add a log handler that write to a file and return the handler "
    rootLog = logging.getLogger('')
    fh = logging.FileHandler(logFileName)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    rootLog.addHandler(fh)
    return fh

debugMode=False

def setupLogging(progName, options, parser=None, logFileName=None, \
        debug=False, fileLevel=logging.DEBUG, minimumLog=False, fileMode="w"):
    """ direct logging to a file and also to stdout, depending on options (debug, verbose, jobId, etc) """
    assert(progName!=None)
    global debugMode
    logging.addLevelName(5,"VERBOSE")

    if options!=None and "cluster" in options.__dict__ and options.cluster!=None:
        global forceHeadnode
        # for makeClusterRunner
        forceHeadnode = options.cluster

    if options!=None and "keepTemp" in options.__dict__ and options.keepTemp==True:
        maxCommon.keepTemp=True

    if options==None:
        stdoutLevel=logging.DEBUG

    elif "verbose" in options.__dict__ and options.verbose:
        stdoutLevel=3
        fileLevel = 3
        debugMode = True

    elif options.debug or debug:
        stdoutLevel=logging.DEBUG
        debugMode = True

    elif minimumLog:
        stdoutLevel=logging.ERROR

    elif "logDir" in options.__dict__ and options.logDir:
        stdoutLevel=logging.CRITICAL
        fileLevel = logging.INFO
        debugMode = True

        timeStr = strftime("_%Y-%m-%d_%H%M%S", gmtime())
        logFileName = join(options.logDir, progName+timeStr)
        logging.warn("Writing message log to %s" % logFileName)

    else:
        stdoutLevel=logging.INFO

    rootLog = logging.getLogger('')
    rootLog.setLevel(fileLevel)
    logging.root.handlers = []

    logging.verbose = verboseFunc
    # setup file logger
    if logFileName != None and logFileName!="":
        fh = logging.FileHandler(logFileName)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        rootLog.addHandler(fh)

    #if logFileName != None:
        #logging.basicConfig(level=fileLevel,
                            #format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            #datefmt='%m-%d %H:%M',
                            #filename= logFileName,
                            #filemode=fileMode, stream=None)
    # define a handler which writes messages to sys.stderr
    console = logging.StreamHandler()
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(levelname)-8s-%(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    console.setLevel(stdoutLevel)
    # make sure that the root logger gets verbose messages
    logging.getLogger('').setLevel(min(stdoutLevel, fileLevel))
    # add the handler to the root logger
    rootLog.addHandler(console)

def printOut(stdout, stderr):
    """ send two strings to logging """
    if len(stdout)!=0:
        logging.debug("stdout: %s" % stdout)
    if len(stderr)!=0:
        if len(stderr) > 1000 or "a4 is redefined" in stderr:
            logging.info("not showing stderr, too big or containes annoying message")
        else:
            logging.info("Size of stderr: %d" % len(stderr))
            logging.info("stderr: %s" % stderr)

def runConverter(cmdLine, fileContent, fileExt, tempDir):
    """ create local temp for in and output files, write data to infile, run
    command. file can be supplied as a str in fileContent["content"] or
    alternatively as a pathname via 'locFname' """
    # create temp file
    fd, inFname = tempfile.mkstemp(suffix="."+fileExt, dir=tempDir, prefix="pubConvPmc.in.")
    logging.debug("Created %s" % inFname)
    maxCommon.delOnExit(inFname)

    inFile = os.fdopen(fd, "wb")
    inFile.write(fileContent)
    inFile.close()

    logging.debug("Created in temp file %s" % (inFname))

    fd, outFname = tempfile.mkstemp(suffix=".txt", dir=tempDir, prefix="pubConvPmc.out.")
    maxCommon.delOnExit(outFname)
    os.close(fd)
    logging.debug("Created out temp file %s" % (outFname))

    # allow %(name)s syntax in cmdLine string to use variables from pubConf
    cmdLine = cmdLine % pubConf.__dict__
    # build cmd line and run
    cmdLine = cmdLine.replace("$in", inFname)
    cmdLine = cmdLine.replace("$out", outFname)
    logging.debug("running "+cmdLine)
    skipFile=False
    stdout, stderr, ret = runCommandTimeout(cmdLine, bufSize=10000000, timeout=30)

    asciiData = None

    if ret==2 and "docx2txt" not in cmdLine: # docx2text returns exit code 2 in some cases
        logging.error("stopped on errno 2: looks like you pressed ctrl-c")
        sys.exit(2)

    if ret!=0:
        logging.error("error %d occured while executing %s" % (ret, cmdLine))
        printOut(stdout, stderr)
        skipFile=True
    if os.path.getsize(outFname)==0:
        logging.error("zero file size of output file after command %s" % (cmdLine))
        printOut(stdout, stderr)
        skipFile=True

    if not skipFile:
        asciiData = open(outFname).read()

    logging.debug("Removing %s" % inFname)
    os.remove(inFname)
    os.remove(outFname)

    asciiData = forceToUnicode(asciiData)

    if skipFile:
        return None
    else:
        return asciiData

# http://stackoverflow.com/questions/92438/stripping-non-printable-characters-from-a-string-in-python
def countBadChars(string):
    """ count special chars in string, but not tab, cr, nl, etc """
    #print "COUNT BAD CHARS", repr(string)
    badCharCount = len(list(control_char_re.finditer(string)))
    #print "BAD CHARS", badCharCount
    return badCharCount

# http://stackoverflow.com/questions/92438/stripping-non-printable-characters-from-a-string-in-python
def removeBadChars(string):
    """ replace bad chars with spaces """
    return control_char_re.sub(" ", string)

def getFileExt(fileData, locFname, mimeType):
    " try to determine best externsion for a file given its fileData dict "
    url = fileData["url"]
    fileExt = None

    logging.debug("trying mime type to get extension")
    # get the mimeType, either local or from dict
    if mimeType==None and "mimeType" in fileData and fileData["mimeType"]!=None:
        mimeType = fileData["mimeType"]
        logging.debug("mime type is %s" % mimeType)

    # get extensions from mime TYpe
    if mimeType!=None:
        fileExt = pubConf.MIMEMAP.get(mimeType, None)
        logging.debug("File extension based on mime type %s" % mimeType)

    # this used to be different - did it break something?
    if fileExt==None:
        if locFname:
            logging.debug("File extension taken from local file %s" % locFname)
            filePath = locFname
        else:
            logging.debug("File extension taken from url %s" % url)
            filePath = url

        fileExt = os.path.splitext(filePath)[1].lower().strip(".")

    logging.debug("File extension determined as  %s" % fileExt)
    return fileExt

def toAscii(fileData, mimeType=None, \
        maxBinFileSize=pubConf.maxBinFileSize, maxTxtFileSize=pubConf.maxTxtFileSize, \
        minTxtFileSize=pubConf.minTxtFileSize):
    """ pick out the content from the fileData dictionary,
    write it to a local file in tempDir and convert it to
    ASCII format. Put output back into the content field.

    mimeType will be used if specified, otherwise try to guess
    converter based on url file extension

    returns fileData if successful, otherwise None
    returns only unicode strings (despite the name)
    """
    converters = pubConf.getConverters()
    tempDir = pubConf.getTempDir()

    fileContent = fileData["content"]
    fileSize = len(fileContent)

    if "locFname" in fileData:
        locFname=fileData["locFname"]
        fileDebugDesc = fileData["externalId"]+":"+locFname
    else:
        locFname = None
        fileDebugDesc = ",".join([fileData["url"],fileData["desc"],
            fileData["fileId"],fileData["articleId"]])

    if fileSize > maxBinFileSize:
        logging.warn("binary file size before conversion %d > %d, skipping file %s" % \
            (len(fileContent), maxBinFileSize, fileDebugDesc))
        return None

    fileExt = getFileExt(fileData, locFname, mimeType)

    if fileExt not in converters:
        logging.debug("Could not convert file %s, no converter for extension %s" % \
            (fileDebugDesc, fileExt))
        return None
    cmdLine = converters[fileExt]

    if cmdLine=="COPY":
        # fileData["content"] already contains ASCII text
        pass

    elif cmdLine=="XMLTEXT" or cmdLine=="NXMLTEXT":
        logging.debug("stripping XML tags")
        if cmdLine=="NXMLTEXT":
            asciiData = pubXml.stripXmlTags(fileContent, isNxmlFormat=True)
        else:
            asciiData = pubXml.stripXmlTags(fileContent)

        if asciiData==None:
            logging.debug("Could not convert xml to ascii, file %s" % fileData["url"])
            return None
        fileData["content"]=asciiData

    else:
        asciiData = runConverter(cmdLine, fileContent, fileExt, tempDir)

        # try to detect corrupted pdf2text output and run second converter
        if fileExt=="pdf" and \
            ((asciiData==None or len(asciiData)<minTxtFileSize) or countBadChars(asciiData)>=10):
            logging.debug("No data or too many non printable characters in PDF, trying alternative program")
            cmdLine = converters["pdf2"]
            asciiData = runConverter(cmdLine, fileContent, fileExt, tempDir)

        if asciiData==None:
            logging.info("conversion failed for %s" % fileDebugDesc)
            return None
        else:
            fileData["content"]=removeBadChars(asciiData)

    fileData = dictToUnicode(fileData)

    if len(fileData["content"]) > maxTxtFileSize:
        logging.info("ascii file size after conversion too big, ignoring file %s" % fileDebugDesc)
        return None

    if len(fileData["content"]) < minTxtFileSize:
        logging.debug("ascii file size only %d bytes < %d, ignoring %s" % \
            (len(fileData["content"]), minTxtFileSize, fileDebugDesc))
        return None

    #charSet = set(fileData["content"])
    #if len(charSet) < 10:
        #logging.warn("too few characters in ASCII output: %s" % charSet)
        #return None

    return fileData

def toAsciiEscape(fileData, mimeType=None, maxBinFileSize=pubConf.maxBinFileSize, maxTxtFileSize=pubConf.maxBinFileSize, minTxtFileSize=pubConf.minTxtFileSize):
    """ convert to ascii, escape special characters
        returns a fileData dict
    """
    fileData = toAscii(fileData, mimeType=mimeType,\
            maxBinFileSize=maxBinFileSize, maxTxtFileSize=maxTxtFileSize, minTxtFileSize=minTxtFileSize)
    fileData = pubStore.dictToUtf8Escape(fileData)
    return fileData

def stringListToDict(paramList):
    """ splits all strings at '=' and returns a dict from these
    input: ["a=3", "b=bla"]
    output: {"a":"3","b":"bla"}
    """
    paramItems = [string.split("=", 1) for string in paramList]
    paramDict = {}
    for key, val in paramItems:
        paramDict[key] = val
    return paramDict

def readArticleChunkAssignment(inDir, updateIds):
    "read the assignment of articleId -> chunkId from text directory"

    if updateIds==None:
        inFiles = glob.glob(os.path.join(inDir, "*_index.tab"))
    else:
        inFiles = []
        for updateId in updateIds:
            updateId = str(updateId)
            indexFname = "%s_index.tab" % updateId
            if isfile(indexFname):
                inFiles.append(os.path.join(inDir, indexFname))

    if len(inFiles)==0:
        logging.warn("No article chunk assignment")
        return None

    logging.debug("Input files for article -> chunk assignment: %s" % inFiles)

    articleChunks = {}
    for inFile in inFiles:
        logging.info("Parsing %s" % inFile)
        for row in maxCommon.iterTsvRows(inFile):
            chunkId = int(row.chunkId.split("_")[1])
            articleChunks[int(row.articleId)] = int(chunkId)
    return articleChunks

def forceToUnicode(text):
    " force to unicode string: try utf8 first, then latin1 "
    if text==None:
        return None
    if type(text)==str:
        #logging.debug("text is unicode")
        return text
    try:
        text = text.decode("utf8")
    except Exception as err:
        logging.debug("Could not convert to unicode using utf8, problem %s, traceback to stdout" % (err))
        #traceback.print_exception(*sys.exc_info())
        try:
            text = text.decode("latin1")
            logging.debug("Converted using latin1")
        except Exception as err:
            logging.debug("Could not convert to unicode using latin1, problem %s" % err)
            try:
                text = text.decode("cp1252")
            except Exception as err:
                logging.debug("Could not convert to unicode using cp1252, problem %s, traceback to stdout" % (err))
            pass
    return text

def dictToUnicode(dict):
    " forcing all values of dict to unicode strings "
    result = {}
    for key, val in dict.items():
        result[key] = forceToUnicode(val)
    return result

def recursiveSubmit(runner, parameterString):
    """ call the program from sys.argv[0] with the given parameterString
      using maxRun.Runner
    """
    progFile = os.path.abspath(sys.argv[0])
    python = sys.executable
    cmd = "%(python)s %(progFile)s %(parameterString)s" % locals()
    runner.submit(cmd)

def makeClusterRunner(scriptName, maxJob=None, runNow=True, algName=None, headNode=None, maxRam=None, outDir=None):
    """ create a default runner to submit jobs to cluster system
    outDir is the directory where the output files go. A directory
    "<outDir>/clusterBatch" will be created with the tracking info for the
    cluster jobs, e.g. the parasol status files
    """
    scriptBase = splitext(basename(scriptName))[0]

    if outDir is None:
        if algName!=None:
            batchDir = join(pubConf.clusterBatchDir, scriptBase+'-'+algName)
        else:
            batchDir = join(pubConf.clusterBatchDir, scriptBase)
    else:
        batchDir = join(outDir, "clusterBatch")

    if not isdir(batchDir):
        logging.debug("Creating dir %s" % batchDir)
        os.makedirs(batchDir)
    clusterType = pubConf.clusterType

    if headNode==None:
        headNode = pubConf.clusterHeadNode

    if forceHeadnode!=None:
        headNode = forceHeadnode
        logging.info("Headnode set from command line to %s, ignoring default" % headNode)

    if headNode!="localhost":
        logging.info("Preparing cluster run, batchDir %(batchDir)s, default type %(clusterType)s, headNode %(headNode)s" % locals())

    runner = maxRun.Runner(maxJob=maxJob, clusterType=clusterType, \
        headNode=headNode, batchDir = batchDir, runNow=runNow, maxRam=maxRam)
    return runner

def lftpGet(remoteUrl, locDir, fileNames, connCount):
    " use lftp to download files in parallel "
    locDir = os.path.abspath(locDir)
    scriptPath = join(locDir, "lftp.cmd")
    logging.debug("Writing filenames to %s" % scriptPath)
    lFile = open(scriptPath, "w")
    lFile.write("set net:socket-buffer 32000000\n")
    lFile.write("set cmd:parallel %d\n" % int(connCount))
    lFile.write("set xfer:use-temp-file yes\n")  # atomic download
    lFile.write("set xfer:clobber yes\n")
    lFile.write("open %s\n" % remoteUrl)
    lFile.write("set xfer:log true\n")
    lFile.write("lcd %s\n" % locDir)
    pm = maxCommon.ProgressMeter(len(fileNames))
    existDirs = set()
    locNames = []
    for f in fileNames:
        locName = join(locDir, f)
        # make sure that target dir exists
        locFileDir = dirname(locName)
        if locFileDir not in existDirs and not isdir(locFileDir):
            logging.info("Creating directory %s" % locFileDir)
            os.makedirs(locFileDir)
        existDirs.add(locFileDir)

        logging.debug("filename %s" % locName)
        if isfile(locName):
            logging.debug("Already exists: %s, skipping" % locName)
        else:
            lFile.write("get %s -o %s\n" % (f, locName))
            locNames.append(locName)
        pm.taskCompleted()
    lFile.close()

    if find_executable("lftp") is None:
        raise Exception("the command lftp is not in your PATH. Install it wit apt-get install lftp or yum install lftp")

    cmd = ["lftp", "-f", scriptPath]
    logging.debug("Launching lftp for download, cmd %s" % " ".join(cmd))
    ret = subprocess.call(cmd)

    if ret!=0:
        logging.error("error during transfer")
        sys.exit(1)

    logging.debug("Updating downloads.log file in %s" % locDir)
    for f in fileNames:
        appendLog(locDir, "add", f)
    logging.info("Downloaded %d files: %s" % (len(locNames), str(",".join(locNames))))

def appendLog(outDir, change, fname):
    logPath = join(outDir, "downloads.log")
    if not isfile(logPath):
        logFile = open(logPath, "w")
        logFile.write("date\tchange\tfiles\n")
    else:
        logFile = open(logPath, "a")

    timeStr = time.strftime("%x %X")
    row = [timeStr, change, fname]
    logFile.write("\t".join(row))
    logFile.write("\n")
    logFile.close()

def getFtpDir(server, path):
    " return list of files on ftp server "
    logging.info("Getting FTP directory server %s, %s" % (server, path))
    ftp = ftplib.FTP(server, user="anonymous", passwd=pubConf.email, timeout=60)
    ftp.cwd(path)
    dirLines = ftp.nlst()
    #fnames = [split(line)[0] for line in dirLines]
    logging.debug("Found files %s" % dirLines)
    return dirLines


def resolveDatasetDesc(descs):
    " resolve a comma-sep list of dataset identifiers like pmc or elsevier to a list of directories "
    dirs = []
    for desc in descs.split(','):
        descDir = pubConf.resolveTextDir(desc)
        if descDir==None:
            raise Exception("Unknown dataset: %s" % desc)
        dirs.append(descDir)
    return dirs

def splitAnnotId(annotId):
    """
    split the 64bit-annotId into packs of 10/3/5 digits and return all
    >>> splitAnnotId(200616640112350013)
    (2006166401, 123, 50013)
    """
    fileDigits = pubConf.FILEDIGITS
    annotDigits = pubConf.ANNOTDIGITS
    articleDigits = pubConf.ARTICLEDIGITS

    annotIdInt = int(annotId)
    articleId  = int(annotIdInt / 10**(fileDigits+annotDigits))
    fileAnnotId= annotIdInt % 10**(fileDigits+annotDigits)
    fileId     = int(fileAnnotId / 10**(annotDigits))
    annotId    = fileAnnotId % 10**(annotDigits)
    return articleId, fileId, annotId

def splitAnnotIdString(annotIdString):
    """ split annot as a string into three parts
    >>> splitAnnotId("200616640112350013")
    (2006166401, 123, 50013)
    """
    fileDigits = pubConf.FILEDIGITS
    annotDigits = pubConf.ANNOTDIGITS
    articleDigits = pubConf.ARTICLEDIGITS

    articleId = annotIdString[:articleDigits]
    fileId = annotIdString[articleDigits:articleDigits+fileDigits]
    annotId = annotIdString[articleDigits+fileDigits:]
    return articleId, fileId, annotId

def makeTempDir(prefix, tmpDir=None):
    """ create unique local temp subdir in pubtools temp dir.
        the pubtools temp dir is usually located on a local disk or ramdisk.
    """
    if tmpDir==None:
        tmpDir=pubConf.getTempDir()
    dirName = tempfile.mktemp(dir=tmpDir, prefix=prefix+".")
    if not isdir(dirName):
        os.makedirs(dirName)
    logging.debug("Created temporary dir %s" % dirName)
    return dirName

def makeTempFile(prefix, suffix=".psl"):
    """ create tempfile in pubtools tempdir dir with given prefix.
    Return tuple (file object , name).
    Tempfile will auto-delete when file object is destructed, unless debug mode is set.
    """
    tmpDir=pubConf.getTempDir()
    if pubConf.debug:
        tfname = tempfile.mktemp(dir=tmpDir, prefix=prefix+".", suffix=suffix)
        #tfname = join(tmpDir, prefix+suffix)
        tf = open(tfname, "w")
        logging.debug("Created tempfile %s, debug-mode: no auto-deletion" % tfname)
    else:
        tf = tempfile.NamedTemporaryFile(dir=tmpDir, prefix=prefix+".", mode="w", suffix=suffix)
    return tf, tf.name

def concatDelLogs(inDir, outDir, outFname):
    " concat all log files to outFname in outDir and delete them "
    outPath = join(outDir, outFname)
    inMask = join(inDir, "*_*.log")
    logFnames = glob.glob(inMask)
    logging.info("Concatting %d logfiles from %s to %s" % (len(logFnames), inMask, outPath))
    delFnames = []

    if not isfile(outPath):
        ofh = open(outPath, "w")
        for inFname in logFnames:
            ofh.write("---- LOGFILE %s ------\n" % inFname)
            ofh.write(open(inFname).read())
            ofh.write("\n")
            delFnames.append(inFname)
        ofh.close()

    for fname in delFnames:
        os.remove(fname)

def concatIdentifiers(inDir, outDir, outFname):
    " concat all identifiers of *_ids.tab files in inDir to outFname, append if exists "
    outPath = join(outDir, outFname)
    inMask = join(inDir, "*_ids.tab")
    idFnames = glob.glob(inMask)
    logging.debug("Concatting exernalIds from %s to %s" % (inMask, outPath))
    extIds = []
    for inFname in idFnames:
        if os.path.getsize(inFname)==0:
            logging.warn("file %s has zero size")
            continue
        for row in maxCommon.iterTsvRows(inFname):
            extIds.append(row.externalId)

    if isfile(outPath):
        ofh = open(outPath, "a")
    else:
        ofh = open(outPath, "w")
        ofh.write("#externalId\n")

    for extId in extIds:
        ofh.write("%s\n" % extId)
    ofh.close()

    return outPath

def parseDoneIds(fname):
    " parse all already converted identifiers from inDir "
    print(fname)
    doneIds = set()
    if os.path.getsize(fname)==0:
        return doneIds

    for row in maxCommon.iterTsvRows(fname):
        doneIds.add(row.externalId)
    logging.info("Found %d identifiers of already parsed articles" % len(doneIds))
    return doneIds

def concatDelIdFiles(inDir, outDir, outFname):
    """ concat all id files in outDir, write to outFname, delete all id files when finished
    """
    outPath = join(outDir, outFname)
    inMask = join(inDir, "*ids.tab")
    idFnames = glob.glob(inMask)
    logging.debug("Concatting %s to %s" % (inMask, outPath))
    maxTables.concatHeaderTabFiles(idFnames, outPath)
    maxCommon.deleteFiles(idFnames)
    return outPath

lockFnames = []

def setLockFile(outDir, lockName):
    """
    create lock file. die if already exists
    """
    global lockFnames
    lockFname = join(outDir, lockName+".lock")
    if isfile(lockFname):
        raise Exception("The lockfile %s exists. Make sure there is no process already running, "
        "then delete the lock file." % lockFname)
    open(lockFname, "w")
    lockFnames.append(lockFname)
    atexit.register(removeLockFiles)

def removeLockFiles():
    """
    remove all lock files. Die if not exists.
    """
    for lockFname in lockFnames:
        if not isfile(lockFname):
            #raise Exception("%s not found." % lockFname)
            logging.debug("lockfile %s does not exist, skipping" % lockFname)
            continue
        logging.debug("Removing %s" % lockFname)
        os.remove(lockFname)

def setInOutDirs(useDefault, args, pubName):
    """ get in and output dir either from args or create default values,
    depending on useDefault """

    if useDefault:
        inDir, outDir = join(pubConf.extDir, pubName), join(pubConf.textDir, pubName)
    else:
        inDir, outDir = args[:2]
    return inDir, outDir

def makeBuildDir(outDir, mustExist=False):
    ' create a dir outDir/build and error abort if it already exists '
    outDir     = join(outDir, "build")
    if not mustExist and isdir(outDir):
        raise Exception("Directory %s already exists. Looks like a previous conversion run "
            "crashed or is ongoing. Delete the directory and re-run if needed or complete the batch "
            "and use pubConvXXX with the --finish option" % outDir)
    if mustExist and not isdir(outDir):
        raise Exception("Directory %s does not exist." % outDir)

    if not mustExist:
        os.mkdir(outDir)
    return outDir

def saveUpdateInfo(buildDir, updateId, lastArticleId, newIds):
    " save often-needed data to a pickle file in buildDir "
    updInfo = {'updateId' : updateId, 'lastArticleId' : lastArticleId, 'newIds' : newIds}
    updInfoFh = open(join(buildDir, "updateInfo.pickle"), "w")
    logging.info("Saving update info to %s" % updInfoFh.name)
    pickle.dump(updInfo, updInfoFh)

def loadUpdateInfo(buildDir):
    " save often-needed data to a pickle file in buildDir "
    updFname = join(buildDir, "updateInfo.pickle")
    logging.info("loading update info from %s" % updFname)
    updInfoFh = open(updFname)
    updInfo = pickle.load(updInfoFh)
    return updInfo["updateId"], updInfo['lastArticleId'], updInfo['newIds']

def removeUpdateInfo(buildDir):
    updFname = join(buildDir, "updateInfo.pickle")
    logging.debug("Removing %s" % updFname)
    os.remove(updFname)

if __name__=="__main__":
    setupLoggingOptions(None)
    #doctest.testmod()
    test_sectioning()
    #print sectionRes["results"].search("bla\nResults").start()
    #print list(re.compile(r"^Results").findall("bla\nResults"))

# generic functions for all pubtools, like logging, finding files, 
# ascii-conversion, section splitting etc

import os, logging, tempfile, sys, re, unicodedata, subprocess, time, types, traceback, \
    glob, operator, doctest, ftplib
import pubConf, pubXml, maxCommon, orderedDict, pubStore, maxRun
from os.path import *

# for countBadChars
all_chars = (unichr(i) for i in xrange(0x110000))
specCodes = set(range(0,32))
goodCodes = set([7,9,10,11,12,13]) # BELL, TAB, LF, NL, FF (12), CR are not counted
badCharCodes = specCodes - goodCodes 
control_chars = ''.join(map(unichr, badCharCodes))
control_char_re = re.compile('[%s]' % re.escape(control_chars))

class Timeout(Exception):
    pass

def runCommandTimeout(command, timeout=30, bufSize=64000):
    """
    runs command, returns after timeout
    print run(["ls", "-l"])
    print run(["find", "/"], timeout=3) #should timeout
    """
    logging.log(5, "running command %s" % command)
    proc = subprocess.Popen(command, bufsize=bufSize, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    poll_seconds = .250
    deadline = time.time()+timeout
    while time.time() < deadline and proc.poll() == None:
        time.sleep(poll_seconds)

    if proc.poll() == None:
        if float(sys.version[:3]) >= 2.6:
            proc.terminate()
        #raise Timeout()
        logging.error("process %s timed out" % (str(command)))

    stdout, stderr = proc.communicate()
    return stdout, stderr, proc.returncode

def relpath(target, base=os.curdir):
    """
    Return a relative path to the target from either the current dir or an optional base dir.
    Base can be a directory specified either as absolute or relative to current dir.
    BACKPORT for <= python2.6
    """

    if target==base:
        return ""
    if not os.path.exists(target):
        raise OSError, 'Target does not exist: '+target

    if not os.path.isdir(base):
        raise OSError, 'Base is not a directory or does not exist: '+base

    base_list = (os.path.abspath(base)).split(os.sep)
    target_list = (os.path.abspath(target)).split(os.sep)

    # On the windows platform the target may be on a completely different drive from the base.
    if os.name in ['nt','dos','os2'] and base_list[0] <> target_list[0]:
        raise OSError, 'Target is on a different drive to base. Target: '+target_list[0].upper()+', base: '+base_list[0].upper()

    # Starting from the filepath root, work out how much of the filepath is
    # shared by base and target.
    for i in range(min(len(base_list), len(target_list))):
        if base_list[i] <> target_list[i]: break
    else:
        # If we broke out of the loop, i is pointing to the first differing path elements.
        # If we didn't break out of the loop, i is pointing to identical path elements.
        # Increment i so that in all cases it points to the first differing path elements.
        i+=1

    rel_list = [os.pardir] * (len(base_list)-i) + target_list[i:]
    return os.path.join(*rel_list)

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
                    result.add( (relDir, fullPath) )
    logging.info("Found %d files" % len(result))
    return result


def setupLoggingOptions(options):
    setupLogging("", options)
    
def setupLogging(PROGNAME, options, logFileName=False, debug=False, fileLevel=logging.DEBUG, minimumLog=False, fileMode="w"):
    """ direct logging to a file and also to stdout, depending on options (debug, verbose, jobId, etc) """
    if options==None:
        stdoutLevel=logging.DEBUG
    elif "verbose" in options.__dict__ and options.verbose:
        stdoutLevel=5
    elif options.debug or debug:
        stdoutLevel=logging.DEBUG
    elif minimumLog:
        stdoutLevel=logging.ERROR
    else:
        stdoutLevel=logging.INFO

    rootLog = logging.getLogger('')
    rootLog.setLevel(fileLevel)

    logging.root.handlers = []
    if logFileName:
        logging.basicConfig(level=fileLevel,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename= logFileName,
                            filemode=fileMode, stream=None)
    # define a handler which writes messages to sys.stderr
    console = logging.StreamHandler()
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(levelname)-8s-%(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    console.setLevel(stdoutLevel)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

def runConverter(cmdLine, fileContent, fileExt, tempDir):
    """ create in and output files, write data to infile, run command"""
    fd, inFname = tempfile.mkstemp(suffix="."+fileExt, dir=tempDir, prefix="pubConvPmc.in.")
    inFile = os.fdopen(fd, "wb")
    inFile.write(fileContent)
    inFile.close()

    fd, outFname = tempfile.mkstemp(suffix=".txt", dir=tempDir, prefix="pubConvPmc.out.")
    os.close(fd)
    logging.debug("Created temp files %s and %s" % (inFname, outFname))

    cmdLine = cmdLine.replace("$in", inFname)
    cmdLine = cmdLine.replace("$out", outFname)
    logging.debug("running "+cmdLine)
    skipFile=False
    #ret = os.system(cmdLine)
    stdout, stderr, ret = runCommandTimeout(cmdLine, bufSize=10000000, timeout=30)
    if len(stdout)!=0:
        logging.debug("stdout: %s" % stdout)
    if len(stderr)!=0:
        logging.debug("stderr: %s" % stderr)
    asciiData = None

    if ret==2:
        logging.error("stopped on errno 2: looks like you pressed ctrl-c")
        os.remove(inFname)
        os.remove(outFname)
        sys.exit(2)

    if ret!=0:
        logging.error("error %d occured while executing %s" % (ret, cmdLine))
        logging.error("output streams are")
        logging.error("stdout: %s" % stdout)
        logging.error("stderr: %s" % stderr)
        skipFile=True
    if os.path.getsize(outFname)==0:
        logging.error("zero file size of output file after command %s" % (cmdLine))
        logging.error("output streams are")
        logging.error("stdout: %s" % stdout)
        logging.error("stderr: %s" % stderr)
        skipFile=True

    if not skipFile:
        asciiData = open(outFname).read()

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

def toAscii(fileData, mimeType=None, maxBinFileSize=pubConf.MAXBINFILESIZE, maxTxtFileSize=pubConf.MAXTXTFILESIZE):
    """ pick out the content from the fileData dictionary, 
    write it to a local file in tempDir and convert it to 
    ASCII format. Put output back into the content field 

    hint specifies where the files come from. can be elsevier or pmc.
    mimeType will be used if specified, otherwise try to guess
    converter based on url file extension

    returns fileData if successful, otherwise None
    returns only unicode strings (despite the name)
    """
    converters = pubConf.getConverters()
    tempDir = pubConf.getTempDir()

    fileContent = fileData["content"]
    if len(fileContent) > maxBinFileSize:
        logging.warn("binary file size before conversion %d > %d, skipping file %s" % (len(fileContent), maxBinFileSize, fileData["url"]+fileData["desc"]+fileData["fileId"]+fileData["articleId"]))
        return None

    url = fileData["url"]

    fileExt=None
    if mimeType:
        fileExt = pubConf.MIMEMAP.get(mimeType, None)
        logging.debug("File extension determined as %s" % fileExt)
    if fileExt==None:
        fileExt = os.path.splitext(url)[1].lower().strip(".")

    if fileExt not in converters:
        logging.debug("Could not convert file %s, no converter for extension %s" % (url, fileExt))
        return None

    cmdLine = converters[fileExt]

    if cmdLine=="COPY":
        pass
        
    elif cmdLine=="XMLTEXT" or cmdLine=="NXMLTEXT":
        logging.debug("stripping XML tags")
        if cmdLine=="NXMLTEXT":
            asciiData = pubXml.stripXmlTags(fileContent, isNxmlFormat=True)
        else:
            asciiData = pubXml.stripXmlTags(fileContent)

        if asciiData==None:
            logging.warn("Could not convert xml to ascii")
            return None
        fileData["content"]=asciiData
    else:
        asciiData = runConverter(cmdLine, fileContent, fileExt, tempDir)
        if fileExt=="pdf" and (asciiData==None or countBadChars(asciiData)>=10):
            logging.debug("No data or too many non printable characters in PDF, trying alternative program")
            cmdLine = converters["pdf2"]
            asciiData = runConverter(cmdLine, fileContent, fileExt, tempDir)

        if asciiData==None:
            return None
        else:
            fileData["content"]=removeBadChars(asciiData)

    fileData = dictToUnicode(fileData)

    if len(fileData["content"]) > maxTxtFileSize:
        logging.warn("ascii file size after conversion too big, ignoring file")
        return None

    return fileData

def toAsciiEscape(fileData, hint=None, mimeType=None, maxBinFileSize=pubConf.MAXBINFILESIZE, maxTxtFileSize=pubConf.MAXTXTFILESIZE ):
    """ convert to ascii, escape special characters 
        returns a fileData dict
    """
    fileData = toAscii(fileData, mimeType=mimeType, maxBinFileSize=maxBinFileSize, maxTxtFileSize=maxTxtFileSize)
    if fileData==None:
        return None
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
            inFiles.append(os.path.join(inDir, "%s_index.tab" % updateId))

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
    if type(text)==types.UnicodeType:
        #logging.debug("text is unicode")
        return text
    try:
        text = text.decode("utf8")
    except Exception, err:
        logging.debug("Could not convert to unicode using utf8, problem %s, traceback to stdout" % (err))
        #traceback.print_exception(*sys.exc_info())
        try:
            text = text.decode("latin1")
            logging.debug("Converted using latin1")
        except Exception, err:
            logging.debug("Could not convert to unicode using latin1, problem %s" % err)
            try:
                text = text.decode("cp1252")
            except Exception, err:
                logging.debug("Could not convert to unicode using cp1252, problem %s, traceback to stdout" % (err))
            pass
    return text

def dictToUnicode(dict):
    " forcing all values of dict to unicode strings "
    result = {}
    for key, val in dict.iteritems():
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

def makeClusterRunner(scriptName, maxJob=None, runNow=True):
    " create a default runner to submit jobs to cluster system "
    scriptBase = splitext(basename(scriptName))[0]
    batchDir = join(pubConf.clusterBatchDir, scriptBase)
    clusterType = pubConf.clusterType
    headNode = pubConf.clusterHeadNode
    logging.info("Preparing cluster run, batchDir %(batchDir)s, type %(clusterType)s, headNode %(headNode)s" % locals())
    runner = maxRun.Runner(maxJob=pubConf.convertMaxJob, clusterType=clusterType, \
        headNode=headNode, batchDir = batchDir, runNow=runNow)
    return runner

def lftpGet(remoteUrl, locDir, fileNames, connCount):
    " use lftp to download files in parallel "
    scriptPath = join(locDir, "lftp.cmd")
    logging.info("Writing filenames to %s" % scriptPath)
    lFile = open(scriptPath, "w")
    lFile.write("set net:socket-buffer 4000000\n")
    lFile.write("set cmd:parallel %d\n" % int(connCount))
    lFile.write("open %s\n" % remoteUrl)
    lFile.write("set xfer:log true\n")
    lFile.write("lcd %s\n" % locDir)
    pm = maxCommon.ProgressMeter(len(fileNames))
    existDirs = set()
    for f in fileNames:
        locName = join(locDir, f)
        # make sure that target dir exists
        locFileDir = dirname(locName)
        if locFileDir not in existDirs and not isdir(locFileDir):
            logging.info("Creating directory %s" % locFileDir)
            os.makedirs(locFileDir)
        existDirs.add(locFileDir)

        if isfile(locName):
            logging.debug("Already exists: %s, skipping" % locName)
        else:
            lFile.write("get %s -o %s\n" % (f, locName))
        pm.taskCompleted()
    lFile.close()

    cmd = ["lftp", "-f", scriptPath]
    logging.info("Launching lftp for download, cmd %s" % " ".join(cmd))
    ret = subprocess.call(cmd)

    if ret!=0:
        logging.error("error during transfer")
        sys.exit(1)

    logging.info("Updating downloads.log file in %s" % locDir)
    for f in fileNames:
        appendLog(locDir, "add", f)
    logging.info("Downloaded %d files" % len(fileNames))

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
    ftp = ftplib.FTP(server, user="anonymous", passwd=pubConf.email)
    ftp.cwd(path)
    dirLines = ftp.nlst() 
    #fnames = [split(line)[0] for line in dirLines]
    logging.debug("Found files %s" % dirLines)
    return dirLines


# SECTIONING OF TEXT

# examples of articles where this DOES NOT work:
#  unzip /hive/data/outside/literature/ElsevierConsyn/2-00290-FULL-XML.ZIP 0140-6736/S0140673600X41633/S0140673686920064/S0140673686920064.xml - raw text, no linebreaks (but dots)

# regular expressions more or less copied from Ruihua Fang, based on her textpresso code
# see BMC http://www.biomedcentral.com/1471-2105/13/16/abstract
flags = re.IGNORECASE | re.UNICODE | re.MULTILINE
prefix = "^[\s\d.IVX]*"
sectionRes = {
    'abstract' :
    re.compile(r"%s(abstract|summary)\s*($|:)" % prefix, flags),
    'intro' :
    re.compile(r"%s(introduction|background)\s*($|:)" % prefix, flags),
    'methods':
    re.compile(r"%s(materials?\s*and\s*methods|patients and methods|methods|experimental\s*procedures|experimental\s*methods)\s*($|:)" % prefix, flags),
    'results':
    re.compile(r"%s(results|case report|results\s*and\s*discussion|results\/discussion)\s*($|:)" % prefix, flags),
    'discussion':
    re.compile(r"%sdiscussion\s*($|:)" % prefix, flags),
    'conclusions':
    re.compile(r"%s(conclusion|conclusions|concluding\s*remarks)\s*($|:)" % prefix, flags),
    'ack':
    re.compile(r"%s(acknowledgment|acknowledgments|acknowledgement|acknowledgements)\s*($|:)" % prefix, flags),
    'refs':
    re.compile(r"%s(literature\s*cited|references|bibliography|refereces|references\s*and\s*notes)\s*($|:)" % prefix, flags)
}

def sectionRanges(text):
    """
    split text into  sections 'header', 'abstract', 'intro', 'results', 'discussion', 'methods', 'ack', 'refs', 'conclusions', 'footer'
    return as ordered dictionary sectionName -> (start, end) tuple
    >>> sectionRanges("Introduction\\nResults\\n\\nReferences\\nNothing\\nAcknowledgements")
    OrderedDict([('header', (0, 0)), ('intro', (0, 13)), ('results', (13, 21)), ('refs', (21, 41)), ('ack', (41, 57))])
    """
    text = text.replace("\a", "\n")
    # get start pos of section headers, create list ('header',0), ('discussion', 400), etc
    sectionStarts = []
    for section, regex in sectionRes.iteritems():
        logging.debug("Looking for %s" % section)
        for match in regex.finditer(text):
            logging.debug("Found at %d" % match.start())
            sectionStarts.append((section, match.start()))
    sectionStarts.sort(key=operator.itemgetter(1))
    sectionStarts.insert(0, ('header', 0))
    sectionStarts.append((None, len(text)))

    # convert to dict of starts for section
    # create dict like {'discussion' : [200, 500, 300]}
    sectionStartDict = orderedDict.OrderedDict()
    for section, secStart in sectionStarts:
        sectionStartDict.setdefault(section, [])
        sectionStartDict[section].append( secStart )

    if len(sectionStartDict)-2<2:
        logging.warn("Fewer than 2 sections, found %s, aborting sectioning" % sectionStarts)
        return None

    # convert to list with section -> (best start)
    bestSecStarts = []
    for section, starts in sectionStartDict.iteritems():
        if len(starts)>2:
            logging.warn("Section %s appears more than twice, aborting sectioning" % section)
            return None
        if len(starts)>1:
            logging.debug("Section %s appears more than once, using only second instance" % section)
            startIdx = 1
        else:
            startIdx = 0
        bestSecStarts.append( (section, starts[startIdx]) )
    logging.debug("best sec starts %s" % bestSecStarts)

    # skip sections that are not in order
    filtSecStarts = []
    lastStart = 0
    for section, start in bestSecStarts:
        if start >= lastStart:
            filtSecStarts.append( (section, start) )
            lastStart = start
    logging.debug("filtered sec starts %s" % filtSecStarts)

    # convert to dict with section -> start, end
    secRanges = orderedDict.OrderedDict()
    for i in range(0, len(filtSecStarts)-1):
        section, secStart = filtSecStarts[i]
        secEnd = filtSecStarts[i+1][1]
        secRanges[section] = (secStart, secEnd)

    # bail out if any section but [header, footer] is of unusual size
    maxSectSize = int(0.7*len(text))
    minSectSize = int(0.003*len(text))
    for section, secRange in secRanges.iteritems():
        if section=='header' or section=='footer':
            continue
        start, end = secRange
        secSize = end - start
        if secSize > maxSectSize:
            logging.warn("Section %s too long, aborting sectioning" % section)
            return None
        elif secSize < minSectSize and section not in ["abstract", "ack"]:
            logging.warn("Section %s too short, aborting sectioning" % section)
            return None
        else:
            pass

    logging.debug("Sectioning OK, found %s" % secRanges)
    return secRanges

def test_sectioning():
    text = "hihihi sflkjdf\n Results and Discussion\nbla bla bla\nI. Methods\n123. Bibliography\n haha ahahah ahah test test\n"
    sec = sectionRanges(text)
    print sec

if __name__=="__main__":
    setupLoggingOptions(None)
    #doctest.testmod()
    test_sectioning()
    #print sectionRes["results"].search("bla\nResults").start()
    #print list(re.compile(r"^Results").findall("bla\nResults"))

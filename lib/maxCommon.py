import logging, os, sys, tempfile, csv, collections, types, codecs, gzip, \
    os.path, re, glob, time, urllib2, doctest, httplib, socket, StringIO
from types import *

def errAbort(text):
    raise Exception(text)
    
def mustExistDir(path, makeDir=False):
    if not os.path.isdir(path):
        if makeDir:
            logging.info("Creating directory %s" % path)
            os.makedirs(path)
        else:
            logging.error("Directory %s does not exist" % path)
            raise Exception()

def mustExist(path):
    if not (os.path.isdir(path) or os.path.isfile(path)):
        logging.error("%s is not a directory or file" % path)
        sys.exit(1)

def mustNotExist(path):
    if (os.path.isdir(path) or os.path.isfile(path)):
        logging.error("%s already exists" % path)
        sys.exit(1)

def mustNotBeEmptyDir(path):
    mustExist(path)
    fileList = os.listdir(path)
    if len(fileList)==0:
        logging.error("dir %s does not contain any files" % path)
        sys.exit(1)

def mustBeEmptyDir(path, makeDir=False):
    " exit if path does not exist or it not empty. do an mkdir if makeDir==True "
    if type(path)==types.ListType:
        for i in path:
            mustBeEmptyDir(i, makeDir=makeDir)
    else:
        if not os.path.isdir(path):
            if not makeDir:
                raise Exception("Directory %s does not exist" % path)
            else:
                logging.info("Creating directory %s" % path)
                os.makedirs(path)
        else:
            if len(os.listdir(path))!=0:
                raise Exception("Directory %s is not empty" % path)

def makeTempFile(tmpDir=None, prefix="tmp", ext=""):
    """ return a REAL temporary file object
    the user is responsible for deleting it!
    """
    fd, filename = tempfile.mkstemp(suffix=ext, dir=tmpDir, prefix=prefix)
    fileObject = os.fdopen(fd, "wb")
    return fileObject, filename

def joinMkdir(*args):
    """ join paths like os.path.join, do an mkdir, ignore all errors """
    path = os.path.join(*args)
    if not os.path.isdir(path):
        logging.debug("Creating dir %s" % path)
        os.makedirs(path)
    return path

def iterCsvRows(path, headers=None):
    " iterate over rows of csv file, uses the csv.reader, see below for homemade version "
    Rec = None
    for row in csv.reader(open(path, "rb")):
        if headers == None:
            headers = row
            headers = [re.sub("[^a-zA-Z]","_", h) for h in headers]
            Rec = collections.namedtuple("iterCsvRow", headers)
            continue
        fields = Rec(*row)
        yield fields

def iterTsvDir(inDir, ext=".tab.gz", headers=None, format=None, fieldTypes=None, \
            noHeaderCount=None, encoding="utf8", fieldSep="\t", onlyFirst=False):
    " run iterTsvRows on all .tab or .tab.gz files in inDir "
    inMask = os.path.join(inDir, "*"+ext)
    inFnames = glob.glob(inMask)
    logging.debug("Found files %s" % inFnames)
    pm = ProgressMeter(len(inFnames))
    if len(inFnames)==0:
        raise Exception("No file matches %s" % inMask)

    for inFname in inFnames:
        for row in iterTsvRows(inFname, headers, format, fieldTypes, noHeaderCount, encoding, fieldSep):
            yield row
        pm.taskCompleted()
        if onlyFirst:
            break

def iterTsvRows(inFile, headers=None, format=None, noHeaderCount=None, fieldTypes=None, encoding="utf8", fieldSep="\t", isGzip=False):
    """ 
        parses tab-sep file with headers as field names 
        yields collection.namedtuples
        strips "#"-prefix from header line

        if file has no headers: 
        
        a) needs to be called with 
        noHeaderCount set to number of columns.
        headers will then be named col0, col1, col2, col3, etc...

        b) you can also set headers to a list of strings
        and supply header names in that way.
    
        c) set the "format" to one of: psl, this will also do type conversion

        fieldTypes can be a list of types.xxx objects that will be used for type
        conversion (e.g. types.IntType)
    """

    if noHeaderCount:
        numbers = range(0, noHeaderCount)
        headers = ["col" + unicode(x) for x in numbers]

    if format=="psl":
        headers =      ["score", "misMatches", "repMatches", "nCount", "qNumInsert", "qBaseInsert", "tNumInsert", "tBaseInsert", "strand",    "qName",    "qSize", "qStart", "qEnd", "tName",    "tSize", "tStart", "tEnd", "blockCount", "blockSizes", "qStarts", "tStarts"]
        fieldTypes =   [IntType, IntType,      IntType,      IntType,  IntType,      IntType,       IntType,       IntType,      StringType,  StringType, IntType, IntType,  IntType,StringType, IntType, IntType,  IntType,IntType ,     StringType,   StringType,StringType]

    if isinstance(inFile, str):
        if inFile.endswith(".gz") or isGzip:
            zf = gzip.open(inFile, 'rb')
            reader = codecs.getreader(encoding)
            fh = reader(zf)
        else:
            fh = codecs.open(inFile, encoding=encoding)
    else:
        fh = inFile

    if headers==None:
        line1 = fh.readline()
        line1 = line1.strip("\n").strip("#")
        headers = line1.split(fieldSep)
        headers = [re.sub("[^a-zA-Z0-9_]","_", h) for h in headers]

    Record = collections.namedtuple('tsvRec', headers)
    for line in fh:
        fields = line.strip("\n").split(fieldSep)
        #fields = [x.decode(encoding) for x in fields]
        if fieldTypes:
            fields = [f(x) for f, x in zip(fieldTypes, fields)]
        try:
            rec = Record(*fields)
        except Exception, msg:
            logging.error("Exception occured while parsing line, %s" % msg)
            logging.error("Filename %s" % fh.name)
            logging.error("Line was: %s" % line)
            logging.error("Does number of fields match headers?")
            logging.error("Headers are: %s" % headers)
            sys.exit(1)
        # convert fields to correct data type
        yield rec

def iterTsvGroups(fileObject, **kwargs):
    """ 
    iterate over a tab sep file, convert lines to namedtuples (records), group lines by some field.

    file needs to be sorted on this field!
    parameters:
        groupFieldNumber: number, the index (int) of the field to group on
        useChar: number, only use the first X chars of the groupField

    return:
        (groupId, list of namedtuples)
    """
    groupFieldNumber = kwargs.get("groupFieldNumber", 0)
    useChars = kwargs.get("useChars", None)
    if "groupFieldNumber" in kwargs:
        del kwargs["groupFieldNumber"]
    if useChars:
        del kwargs["useChars"]
    assert(groupFieldNumber!=None)

    lastId = None
    group = []
    id = None
    for rec in iterTsvRows(fileObject, **kwargs):
        id = rec[groupFieldNumber]
        if useChars:
            id = id[:useChars]
        if lastId==None:
            lastId = id
        if lastId==id:
            group.append(rec)
        else:
            yield lastId, group
            group = [rec]
            lastId = id
    if id!=None:
        yield id, group
    
def iterTsvJoin(files, **kwargs):
    r"""
    iterate over two sorted tab sep files, join lines by some field and yield as namedtuples
    files need to be sorted on the field!

    parameters:
        groupFieldNumber: number, the index (int) of the field to group on
        useChar: number, only use the first X chars of the groupField

    return:
        yield tuples (groupId, [file1Recs, file2Recs])
    >>> f1 = StringIO.StringIO("id\ttext\n1\tyes\n2\tunpaired middle\n3\tno\n5\tnothing either\n")
    >>> f2 = StringIO.StringIO("id\ttext\n0\tnothing\n1\tvalid\n3\tnot valid\n")
    >>> files = [f1, f2]
    >>> list(iterTsvJoin(files, groupFieldNumber=0))
    [(1, [[tsvRec(id='1', text='yes')], [tsvRec(id='1', text='valid')]]), (3, [[tsvRec(id='3', text='no')], [tsvRec(id='3', text='not valid')]])]
    """
    assert(len(files)==2)
    f1, f2 = files
    iter1 = iterTsvGroups(f1, **kwargs)
    iter2 = iterTsvGroups(f2, **kwargs)
    groupId1, recs1 = iter1.next()
    groupId2, recs2 = iter2.next()
    while True:
        groupId1, groupId2 = int(groupId1), int(groupId2)
        if groupId1 < groupId2:
            groupId1, recs1 = iter1.next()
        elif groupId1 > groupId2:
            groupId2, recs2 = iter2.next()
        else:
            yield groupId1, [recs1, recs2]
            groupId1, recs1 = iter1.next()
            groupId2, recs2 = iter2.next()

def runCommand(cmd, ignoreErrors=False, verbose=False):
    """ run command in shell, exit if not successful """
    msg = "Running shell command: %s" % cmd
    logging.debug(msg)
    if verbose:
        logging.info(msg)
    ret = os.system(cmd)
    if ret!=0:
        if ignoreErrors:
            logging.info("Could not run command %s" % cmd)
            logging.info("Error message ignored, program will continue")
        else:
            raise Exception("Could not run command (Exitcode %d): %s" % (ret, cmd))

def makedirs(path, quiet=False):
    try:
        os.makedirs(path)
    except:
        if not quiet:
            raise 

def appendTsvNamedtuple(filename, row):
    " append a namedtuple to a file. Write headers if file does not exist "
    if not os.path.isfile(filename):
       outFh = open(filename, "w") 
       headers = row._fields
       outFh.write("\t".join(headers)+"\n")
    else:
       outFh = open(filename, "a")
    outFh.write("\t".join(row)+"\n")

def appendTsvDict(filename, inDict, headers):
    " append a dict to a file in the order of headers"
    values = []
    if headers==None:
        headers = inDict.keys()

    for head in headers:
        values.append(inDict.get(head, ""))

    logging.debug("order of headers is: %s" % headers)

    if not os.path.isfile(filename):
       outFh = codecs.open(filename, "w", encoding="utf8") 
       outFh.write("\t".join(headers)+"\n")
    else:
       outFh = codecs.open(filename, "a", encoding="utf8")
    logging.debug("values are: %s" % values)
    outFh.write(u"\t".join(values)+"\n")

def appendTsvOrderedDict(filename, orderedDict):
    appendTsvDict(filename, orderedDict, None)

class ProgressMeter:
    """ prints a message "x%" every stepCount/taskCount calls of taskCompleted()
    """
    def __init__(self, taskCount, stepCount=20, quiet=False):
        self.taskCount=taskCount
        self.stepCount=stepCount
        self.tasksPerMsg = taskCount/stepCount
        self.i=0
        self.quiet = quiet
        #print "".join(9*["."])

    def taskCompleted(self):
        if self.quiet and self.taskCount<=5:
            return
        #logging.debug("task completed called, i=%d, tasksPerMsg=%d" % (self.i, self.tasksPerMsg))
        if self.tasksPerMsg!=0 and self.i % self.tasksPerMsg == 0:
            donePercent = (self.i*100) / self.taskCount
            #print "".join(5*[chr(8)]),
            print ("%.2d%% " % donePercent),
            sys.stdout.flush()
        self.i+=1
        if self.i==self.taskCount:
            print ""

def test():
    pm = ProgressMeter(2000)
    for i in range(0,2000):
        pm.taskCompleted()

def parseConfig(f):
    " parse a name=value file from file-like object f and return as dict"
    if isinstance(f, str):
        logging.debug("parsing config file %s" % f)
        f = open(os.path.expanduser(f))
    result = {}
    for line in f:
        if line.startswith("#"):
            continue
        line = line.strip()
        if "=" in line:
            key, val = line.split("=")
            result[key]=val
    return result

def retryHttpRequest(url, params=None, repeatCount=15, delaySecs=120):
    """ wrap urlopen in try...except clause and repeat
    #>>> retryHttpHeadRequest("http://www.test.com", repeatCount=1, delaySecs=1)
    """
    
    def handleEx(ex, count):
        logging.debug("Got Exception %s, %s on urlopen of %s, %s. Waiting %d seconds before retry..." % \
            (type(ex), str(ex), url, params, delaySecs))
        time.sleep(delaySecs)
        count = count - 1
        return count
        
    socket.setdefaulttimeout(20)
    count = repeatCount
    while count>0:
        try:
            ret = urllib2.urlopen(url, params, 20)
        except urllib2.HTTPError as ex:
            count = handleEx(ex, count)
        except httplib.HTTPException as ex:
            count = handleEx(ex, count)
        except urllib2.URLError as ex:
            count = handleEx(ex, count)
        else:
            return ret

    logging.debug("Got repeatedexceptions on urlopen, returning None")
    return None
    
def retryHttpHeadRequest(url, repeatCount=15, delaySecs=120):
    class HeadRequest(urllib2.Request):
        def get_method(self):
            return u'HEAD'

    response = retryHttpRequest(HeadRequest(url), repeatCount=repeatCount, delaySecs=delaySecs)
    return response
    
def sendEmail(address, subject, text):
    text = text.replace("'","")
    subject = subject.replace("'","")
    cmd = "echo '%s' | mail -s '%s' %s" % (text, subject, address)
    logging.info("Email command %s" % cmd)
    os.system(cmd)

if __name__=="__main__":
    #test()
    import doctest
    doctest.testmod()

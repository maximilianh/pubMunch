# functions to annotate text or use external algorithms (webservices,perlscripts) to annotate text

import logging, subprocess, urllib, urllib2, re, shlex, os, sys, types

# matches for these are removed from the file (=replaced by spaces) 
xmlTagsRe  = re.compile('<.*?>')     # an xml tag 
mathTypeRe = re.compile('MathType@[^ ]*') # a mathtype formula 
 
# for cleaning/splitting the text files into words 
nonLetterRe= re.compile(r'[\W]') # any non-alphanumeric character 
digitRe    = re.compile('[0-9]')  # any digit 
wordRe     = re.compile('[a-zA-Z]+') # any word 

def replaceWithSpaces(regex, string):
    """ replaces all occurrences of regex in string with spaces 
    >>> replaceWithSpaces(xmlTagsRe, "<test> nonTag <test>")
    '       nonTag       '
    """
    def toSpaces(matchObject):
        return "".join([" "]*(matchObject.end(0) - matchObject.start(0)))
    return regex.sub(toSpaces, string) 


def cleanText(text):
    """ replace xml, mathtype tags, and non-letter characters with spaces in a string """
    # clean: xml tags and mathtype -> spaces
    cleanText = replaceWithSpaces(xmlTagsRe, text)
    cleanText = replaceWithSpaces(mathTypeRe, text)
    # clean: non-letters -> spaces
    cleanText = nonLetterRe.sub(" ", cleanText)
    cleanText = digitRe.sub(" ", cleanText)
    return cleanText

class TextAnnotator:
    """ class which annotates a text and returns tab-sep lines """
    def run(self, metaInfo, text):
        pass

    def getHeaders(self):
        pass


class HttpRunner(TextAnnotator):
    """ runs a Restful webservice at url with given parametres.

    parameters is a dictionary with key=value entries to pass via http
    parameters can reference metaInfo fields like $numId 
    parameters must include the text as a field with value $text.
    """

    def __init__(self, url, parameters, headers, fields=None):
        logging.debug("Setting up algorithm at %s with parameters %s" % (url, str(parameters)))
        self.url = url
        self.paramTemplate = parameters
        if fields!=None:
            self.fields    = [int(x) for x in fields.split(",")]
        else:
            self.fields= None

        self.description   = "http webservice at %s with parameters %s" %(url, str(parameters))
        self.headers       = headers.split(",")
        self.RowType       = namedtuple.namedtuple("algRecord", self.headers)

    def getHeaders(self):
        return self.headers

    def run(self, metaInfo, text):
        httpParams = {}
        for key, value in self.paramTemplate.iteritems():
            if value=="$text":
                httpParams[key] = text
            elif value.startswith("$"):
                httpParams[key] = metaInfo._asdict()[value.strip("$")]
            else:
                httpParams[key] = value

        logging.log(5, "HTTP POST to %s with parameters %s" % (self.url, str(httpParams)))
        dataEncoded = urllib.urlencode (httpParams)
        req = urllib2.Request(self.url, dataEncoded)
        response = urllib2.urlopen(req)
        data =  response.read()

        lines = data.split("\n")
        for line in lines:
            logging.log(5, "Received data: %s" % line)
  
        rows = []
        for line in lines:
            if "<message" in line or "<error" in line:
                logging.debug("Got message from url: " + line)
                continue
            if len(line)==0:
                continue

            columns = line.split("\t")

            if self.fields!=None:
                newRow = []
                for i in self.fields:
                    newRow.append(columns[i])
                columns = newRow
            
            row = self.RowType(*columns)
            yield row

def loadPythonObject(moduleFilename, objName):
    """ get function pointer or object from dynamically loaded library """
    # must add path to system search path first
    if not os.path.isfile(moduleFilename):
        logging.error("Could not find %s" % moduleFilename)
        sys.exit(1)
    modulePath, moduleName = os.path.split(moduleFilename)
    moduleName = moduleName.replace(".py","")
    logging.debug("Loading python function/variable %s from %s in dir %s" % (objName, moduleName, modulePath))
    sys.path.append(modulePath)

    # load algMod as a module, copied from 
    # http://code.activestate.com/recipes/223972-import-package-modules-at-runtime/
    try:
        aMod = sys.modules[moduleName]
        if not isinstance(aMod, types.ModuleType):
            raise KeyError
    except KeyError:
        # The last [''] is very important!
        aMod = __import__(moduleName, globals(), locals(), [''])
        sys.modules[moduleName] = aMod
    if not hasattr(aMod, objName):
        return None
    else:
        algFunc = getattr(aMod, objName)
        return algFunc

def pipeDataIntoProgram(commandLine, changeToDir, text):
    """ iterates over text yielded by a DocumentGetter, pipes text into perlCommandLine and returns results"""

    docCount=0

    # start the perl script which detects protein sequences and return as a subprocess object

    text = cleanText(text)
    text.replace("-","")
    text.replace("~","")
    text.replace("(","")
    text.replace(")","")

    #logging.debug("Executing %s" % commandLine)
    args = shlex.split(commandLine)
    #logging.debug("Arguments are %s" % str(args))
    proc = subprocess.Popen(args, bufsize=1000000, stdin=subprocess.PIPE, stdout=subprocess.PIPE, cwd=changeToDir)
    logging.debug("Piping article into program")
    stdoutData, stdinData = proc.communicate(text)
    return stdoutData


class PipeRunner(TextAnnotator):
    """ a TextAnnotator that will run an executable, sends the text to stdin and returns stdout
        Headers have to be configured via a header-parameter
    """
    def __init__(self, dirName, cmdLine):
        self.cmdLine=cmdLine
        self.cwd    =dirName
        self.description = "script in %s : %s" % (dirName, cmdLine)

    def run(self, metaInfo, text):
        logging.debug("piping text into %s, in dir %s" % (self.cmdLine, self.cwd))
        output = pipeDataIntoProgram(self.cmdLine, self.cwd, text)
        lines = output.split("\n")
        for line in lines:
            if line!="":
                yield line.split("\t")

class PythonRunner(TextAnnotator):
    def __init__(self, moduleFilename, algFuncName, parameters):
        logging.info("Initializing plugin python module")
        modParameters      = loadPythonObject(moduleFilename, "parameters")
        self.annotFunc     = loadPythonObject(moduleFilename, algFuncName)
        self.headers       = loadPythonObject(moduleFilename, "headers")
        self.description   = "python module "+moduleFilename+", function "+algFuncName
        if self.annotFunc==None:
            logging.error("Could not find the function %s in %s" % (moduleFilename, algFuncName))
            sys.exit(1)

        for key, val in parameters.iteritems():
            setattr(modParameters, key, val)

    def getHeaders(self):
        return self.headers

    def run(self, metaInfo, text):
        # run python function on all files
        return self.annotFunc(metaInfo, text)

def runAlgorithm(articleReader, algRunner, outFile, headers=None):
    """ executes algRunner onto all articles from articleReader, write to outFile """
    # write headers to output file
    logging.info("Running plugin %s" % algRunner.description)
    if not headers:
        headers= algRunner.getHeaders()
    if not headers:
        logging.error("You need to define a variable 'headers' (a list) in your python module")
        logging.error("It specifies the names of the columns that your module returns")
        sys.exit(1)
    headers.append("snippet")
    headers.insert(0,"#internalArticleNumber")
    headers.insert(1,"articleId")
    headers.insert(2,"fileId")
    headers.insert(3,"annotId")
    outFile.write("\t".join(headers)+"\n")
    lastArticleId=None
    annotId = 0 # a unique id for each annotation of an article

    for metaInfo, filename, text in articleReader.iterate():
        if lastArticleId!=metaInfo.numId:
            annotId=0
        for row in algRunner.run(metaInfo, text):
            start, end = row[:2]
            start, end = int(start), int(end)
            snippet = getSnippet(text, start, end)
            if len(row)!=0:
                row.insert(0, metaInfo.numId)
                row.insert(1, metaInfo.id)
                row.insert(2, filename)
                row.insert(3, str(annotId))
                row.append(snippet)
                row = [str(x) for x in row]
                rowStr = "\t".join(row)
                outFile.write(rowStr)
                outFile.write("\n")
                logging.debug("row is %s" % rowStr)
                annotId+=1
                lastArticleId=metaInfo.numId

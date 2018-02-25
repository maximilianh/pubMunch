# Copyright 2006-2012 Mark Diekhans
"Classes used to implement rules that execute commands and produce files"

import os.path,sys
from pm_pycbio.sys import typeOps,fileOps
from pm_pycbio.exrun import ExRunException,Verb
from pm_pycbio.exrun.Graph import Production,Rule
from pm_pycbio.sys import Pipeline

# FIXME: Auto decompression is not supported, as many programs handle reading
# compressed files and the use of the /proc/ files to get pipe paths causes
# file extension issues.

# FIXME: seems like install could be generalized to any rule, Could the make
# CmdRule only need for a supplied list of commands

# FIXME: the number of different get{In,Out} functions is confusing, and can causes
# errors if the wrong one is used

class FileIn(object):
    """Object used to specified an input File as an argument to a command.
    Using an instance of this object in a command line automatically adds it
    as a requirement.  It also support automatic decompression of the file.

    The prefix attribute is used in construction arguments when an
    option-equals is prepended to the file (--in=fname).  The prefix
    can be specified as an option to the constructor, or in a string concatination
    ("--in="+FileIn(f))."""
    __slots__ = ("file", "prefix", "autoDecompress")

    def __init__(self, file, prefix=None, autoDecompress=True):
        self.file = file
        self.prefix = prefix
        self.autoDecompress = autoDecompress

    def __radd__(self, prefix):
        "string concatiation operator that sets the prefix"
        self.prefix = prefix
        return self

    def __str__(self):
        """return input file argument"""
        if self.prefix == None:
            return self.file.getInPath(self.autoDecompress)
        else:
            return self.prefix + self.file.getInPath(self.autoDecompress)

class FileOut(object):
    """Object used to specified an output File as an argument to a command.  Using
    an instance of this object in a command line automatically adds it as a
    production.  It also support automatic compression of the file.

    The prefix attribute is used in construction arguments when an
    option-equals is prepended to the file (--out=fname).  The prefix
    can be specified as an option to the constructor, or in a string concatenation
    ("--out="+FileOut(f)).
    """
    __slots__ = ("file", "prefix", "autoCompress")

    def __init__(self, file, prefix=None, autoCompress=True):
        self.file = file
        self.prefix = prefix
        self.autoCompress = autoCompress

    def __radd__(self, prefix):
        "string concatiation operator that sets the prefix"
        self.prefix = prefix
        return self

    def __str__(self):
        """return input file argument"""
        if self.prefix == None:
            return self.file.getOutPath(self.autoCompress)
        else:
            return self.prefix + self.file.getOutPath(self.autoCompress)

class File(Production):
    """Object representing a file production. This handles atomic file
    creation. CmdRule will install productions of this class after the
    commands succesfully complete.  It also handles automatic compression of
    output.  This is the default behavior, unless overridden by specifying the
    autoCompress=False option the output functions. """

    # No locking is currently required.  If a file has not been installed,
    # then is is only accessed in the rule by a single thread.  If it has been
    # installed, the decompression pipes are private to a rule.
    # FIXME: make sure the above works

    def __init__(self, path, realPath):
        "realPath is use to detect files accessed from different paths"
        Production.__init__(self, path)
        self.path = path
        self.realPath = realPath # FIXME is this needed
        self.outPath = None
        self.installed = False
        # FIXME: add failed flag

    def __str__(self):
        return self.path

    def getLocalTime(self):
        "modification time of file, or None if it doesn't exist"
        if os.path.exists(self.path):
            return os.path.getmtime(self.path)
        else:
            return None

    def getOutPath(self, autoCompress=True):
        """Get the output name for the file, which is newPath until the rule
        terminates. This will also create the output directory for the file,
        if it does not exist.  One wants to use FileOut() to define
        a command argument.  This should not be used to get the path to a file
        to be opened in the ExRun process, use openOut() instead."""
        if self.installed:
            raise ExRunException("output file already installed: " + self.path)
        if self.outPath == None:
            fileOps.ensureFileDir(self.path)
            self.outPath = self.exrun.getAtomicPath(self.path)
        return self.outPath

    def getInPath(self, autoDecompress=True):
        """Get the input path name of the file.  If a new file has been
        defined using getOutPath(), but has not been installed, it's path is
        return, otherwise path is returned.  One wants to use FileIn() to
        define a command argument.  This should not be used to get the path to
        a file to be opened in the ExRun process, use openIn() instead. """
        if (not self.installed) and (self.outPath != None):
            return self.outPath
        else:
            return self.path

    def openOut(self, autoCompress=True):
        """open the output file for writing from the ExRun process"""
        path = self.__setupOut(autoCompress)
        if fileOps.isCompressed(path) and autoCompress:
            return fileOps.openz(path, "w")
        else:
            return open(path, "w")
        
    def openIn(self, autoDecompress=True):
        """open the input file for reading in the ExRun process"""
        path = self.__setupIn()
        if fileOps.isCompressed(path) and autoDecompress:
            return fileOps.openz(path)
        else:
            return open(path)

    def done(self):
        """called when command completes with success or failure, waits for
        pipes but doesn't install output"""
        pass

    def finishSucceed(self):
        "finish production with atomic install of new output file as actual file"
        self.done()
        if self.installed:
            raise ExRunException("output file already installed: " + self.path)
        if self.outPath == None:
            raise ExRunException("getOutPath() never called for: " + self.path)
        if not os.path.exists(self.outPath):
            raise ExRunException("output file as not created: " + self.outPath)
        fileOps.atomicInstall(self.outPath, self.path)
        self.installed = True
        self.outPath = None

    def finishFail(self):
        """called when rule failed, doesn't install files"""
        self.done()

    def finishRequire(self):
        """Called when the rule that requires this production finishes
        to clean up decompression pipes"""
        self.done()

class Cmd(list):
    """A command in a CmdRule. Contains a list of lists of command words,
    which will either be any type of object or FileIn/FileOut objects.  
    The stdin,stdout, stderr arguments are used for redirect I/O.
    stdin/out/err can be open files, strings, or File production objects.
    If they are File objects, the atomic file handling methods are used
    to get the path.
    """

    def __init__(self, cmd, stdin=None, stdout=None, stderr=None):
        """The str() function is called on each word when assembling arguments
        to a comand, so arguments do not need to be strings."""
        if isinstance(cmd[0], list) or isinstance(cmd[0], tuple):
            for scmd in cmd:
                self.__addSimple(scmd)
        else:
            self.__addSimple(cmd)
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.prodToDev = None   # map of Production to Dev

    def __addSimple(self, scmd):
        "add one simple command"
        if not isinstance(scmd, tuple):
            scmd = tuple(scmd)
        self.append(scmd)

    def __getInput(self, pdag, fspec):
        """Get an input file. If fspec can be None, File or FileIn, or any
        object that can be converted to a string.  Return the appropriate PIn
        object, None, or a string. Adds decompression process if needed."""
        if fspec == None:
            return None
        if isinstance(fspec, File):
            fspec = FileIn(fspec)
        if not isinstance(fspec, FileIn):
            return str(fspec)

        # handle File object, include arg prefix
        path = fspec.file.getInPath()
        if fileOps.isCompressed(path) and fspec.autoDecompress:
            pdev = Pipeline.Pipe()
            pdag.create((fileOps.decompressCmd(path), path), stdout=pdev)
            return Pipeline.PIn(pdev, fspec.prefix)
        else:
            return Pipeline.PIn(Pipeline.File(path), fspec.prefix)


    def __getOutput(self, pdag, fspec):
        """Get an output file. If fspec can be None, File or FileOut, or any
        object that can be converted to a string.  Return the appropriate Pout
        object, None, or a string. Adds compression process if needed."""
        if fspec == None:
            return None
        if isinstance(fspec, File):
            fspec = FileOut(fspec)
        if not isinstance(fspec, FileOut):
            return str(fspec)

        # handle File object, include arg prefix
        path = fspec.file.getOutPath()
        if fileOps.isCompressed(path) and fspec.autoCompress:
            pdev = Pipeline.Pipe()
            pdag.create((fileOps.compressCmd(path),), stdin=pdev, stdout=path)
            return Pipeline.POut(pdev, fspec.prefix)
        else:
            return Pipeline.POut(Pipeline.File(path), fspec.prefix)

    def __createProc(self, pdag, scmd, stdin, stderr, isLast):
        "define one process in the command, return stdout"
        if isLast:
            stdout = self.__getOutput(pdag, self.stdout)
        else:
            stdout = Pipeline.Pipe()
        # convert arguments
        pcmd = []
        for a in scmd:
            if isinstance(a, FileIn):
                pcmd.append(self.__getInput(pdag, a))
            elif isinstance(a, FileOut):
                pcmd.append(self.__getOutput(pdag, a))
            else:
                pcmd.append(str(a))
        pdag.create(pcmd, stdin, stdout, stderr)
        return stdout

    def __createProcDag(self):
        "construct ProcDag for command"
        pdag = Pipeline.ProcDag()
        prevStdout = self.__getInput(pdag, self.stdin)
        stderr = self.__getOutput(pdag, self.stderr)
        last = self[-1]
        for scmd in self:
            prevStdout = self.__createProc(pdag, scmd, prevStdout, stderr, (scmd == last))
        return pdag

    def call(self, verb):
        "run command, with tracing"
        self.prodToDev = dict()
        try:
            pdag = self.__createProcDag()
            if verb.enabled(Verb.trace):
                verb.pr(Verb.trace, str(pdag))
            pdag.wait()
        finally:
            self.prodToDev = None

class PersistentFlag(Production):
    """Object representing a flag file indicating that a rules has succeeded.
    These are stored in the experiment control directory."""
    

class CmdRule(Rule):
    """Rule to execute processes.  Automatically installs File producions after
    completion.  A rule can containing multiple commands, represented as Cmd
    objects, since can be single processes or complex pipelines.  Each Cmd
    object is executed serially, with file products not installed until
    the end.  Output file products can be used in subsequent rule steps,
    being accessed under their temporary names.

    This can be used it two ways, either give a lists of commands which are
    executed, or a rule class can be derived from this that executes the
    command when the rule is evaluated.
    
    If commands are specified to the constructor, they are either a Cmd object
    or a list of Cmd objects.  If the input of the Cmd are File objects, they
    are added to the requires, and output of type File are added to the
    produces.  However, if the input of a command is an output of a previous
    command, it the list, it doesn't become a require, to allow outputs to
    also be inputs of other comments for the rule.

    The derived class overrides run() function to evaulate the rule and uses
    the call() function to execute each command or pipeline.

    Rule name is generated from productions if not specified.
    """

    @staticmethod
    def __mkNamePart(prods):
        if typeOps.isIterable(prods):
            return ",".join(map(str, prods))
        else:
            return str(prods)

    @staticmethod
    def __mkName(requires, produces):
        return "Rule["+ CmdRule.__mkNamePart(requires) + "=>" + CmdRule.__mkNamePart(produces)+"]"

    def __init__(self, cmds=None, name=None, requires=None, produces=None):
        requires = typeOps.mkset(requires)
        produces = typeOps.mkset(produces)

        # deal with commands before super init, so all requires and produces
        # are there for the name generation
        self.cmds = None
        if cmds != None:
            self.cmds = []
            if isinstance(cmds, Cmd):
                self.__addCmd(cmds, requires, produces)
            else:
                for cmd in cmds:
                    self.__addCmd(cmd, requires, produces)
        if name == None:
            name = CmdRule.__mkName(requires, produces)
        Rule.__init__(self, name, requires, produces)

    def __addCmd(self, cmd, requires, produces):
        assert(isinstance(cmd, Cmd))
        self.__addCmdStdio(cmd.stdin, requires, produces)
        self.__addCmdStdio(cmd.stdout, produces)
        self.__addCmdStdio(cmd.stderr, produces)
        for scmd in cmd:
            self.__addCmdArgFiles(scmd, requires, produces)
        self.cmds.append(cmd)

    def __addCmdStdio(self, fspecs, specSet, exclude=None):
        "add None, a single or a list of file specs as requires or produces links"
        for fspec in typeOps.mkiter(fspecs):
            if  (isinstance(fspec, FileIn) or isinstance(fspec, FileOut)):
                fspec = fspec.file  # get File object for reference
            if (isinstance(fspec, File) and ((exclude == None) or (fspec not in exclude))):
                specSet.add(fspec)

    def __addCmdArgFiles(self, cmd, requires, produces):
        """scan a command's arguments for FileIn and FileOut object and add these to
        requires or produces"""
        for a in cmd:
            if isinstance(a, FileIn):
                requires.add(a.file)
            elif isinstance(a, FileOut):
                produces.add(a.file)
            elif isinstance(a, File):
                raise ExRunException("can't use File object in command argument, use FileIn() or FileOut() to generate a reference object")

    def __callDone(self, fil):
        "call done function, if exception, set firstEx if it is None"
        try:
            fil.done()
        except Exception, ex:
            ex = ExRunException("Exception on file: " + str(fil), cause=ex)
            self.verb.pr(Verb.error, str(ex)+"\n"+ex.format())
            return ex
        return None

    def __closeFiles(self, files, firstEx):
        "call done method on files, to close pipes"
        for f in files:
            ex = self.__callDone(f)
            if (ex != None) and (firstEx == None):
                firstEx = ex
        return firstEx

    def call(self, cmd):
        "run a commands with optional tracing"
        firstEx = None
        try:
            try:
                cmd.call(self.verb)
            except Exception, ex:
                ex = ExRunException("Exception running command: " + str(cmd), cause=ex)
                self.verb.pr(Verb.error, str(ex)+"\n"+ex.format())
                firstEx = ex
        finally:
            firstEx = self.__closeFiles(self.requires, firstEx)
            firstEx = self.__closeFiles(self.produces, firstEx)
        if firstEx != None:
            raise firstEx

    def runCmds(self):
        "run commands supplied in the constructor"
        if self.cmds == None:
            raise ExRunException("no commands specified and run() not overridden for CmdRule: " + self.name)
        for cmd in self.cmds:
            self.call(cmd)

    def run(self):
        """run the commands for the rule, the default version runs the
        commands specified at construction, override this for a derived class"""
        self.runCmds()

    def execute(self):
        "execute the rule"
        self.verb.enter()
        try:
            self.run()
        except Exception, ex:
            ex = ExRunException("Exception executing rule: " + str(self), cause=ex)
            self.verb.pr(Verb.error, str(ex)+"\n"+ex.format())
            raise
        finally:
            self.verb.leave()

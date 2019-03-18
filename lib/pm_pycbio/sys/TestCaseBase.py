from __future__ import print_function
# Copyright 2006-2012 Mark Diekhans
from pm_pycbio.sys import fileOps
import os, sys, unittest, difflib, threading, errno, re, glob, subprocess
from pipes import quote

try:
    MAXFD = os.sysconf("SC_OPEN_MAX")
except:
    MAXFD = 256


class TestCaseBase(unittest.TestCase):
    """Base class for test case with various test support functions"""

    def __init__(self, methodName='runTest'):
        """initialize, removing old output files associated with the class"""
        unittest.TestCase.__init__(self, methodName)
        clId = self.getClassId()
        od = self.getOutputDir()
        for f in glob.glob(od+"/"+clId + ".*") + glob.glob(od+"/tmp.*."+clId + ".*"):
            fileOps.rmTree(f)
    
    def getClassId(self):
        """Get the first part of the portable test id, consisting 
        moduleBase.class.  This is the prefix to output files"""  
        # module name is __main__ when run standalone, so get base file name
        mod = os.path.splitext(os.path.basename(sys.modules[self.__class__.__module__].__file__))[0]
        return mod + "." + self.__class__.__name__

    def getId(self):
        """get the fixed test id, which is in the form moduleBase.class.method
        which avoids different ids when test module is run as main or
        from a larger program"""
        # last part of unittest id is method
        return self.getClassId() + "." + self.id().split(".")[-1]

    def getTestDir(self):
        """find test directory, where concrete class is defined."""
        testDir =  os.path.dirname(sys.modules[self.__class__.__module__].__file__)
        if testDir == "":
            testDir = "."
        testDir = os.path.realpath(testDir)
        # turn this into a relative directory
        cwd = os.getcwd()
        if testDir.startswith(cwd):
            testDir = testDir[len(cwd)+1:]
            if len(testDir) == 0:
                testDir = "."
        return testDir

    def getTestRelProg(self, progName):
        "get path to a program in directory above the test directory"
        return os.path.join(self.getTestDir(), "..", progName)

    def getInputFile(self, fname):
        """Get a path to a file in the test input directory"""
        return self.getTestDir() + "/input/" + fname

    def getOutputDir(self):
        """get the path to the output directory to use for this test, create if it doesn't exist"""
        d = self.getTestDir() + "/output";
        fileOps.ensureDir(d)
        return d

    def getOutputFile(self, ext):
        """Get path to the output file, using the current test id and append
        ext, which should contain a dot"""
        f = self.getOutputDir() + "/" + self.getId() + ext;
        fileOps.ensureFileDir(f)
        return f

    def getExpectedFile(self, ext):
        """Get path to the expected file, using the current test id and append ext"""
        return self.getTestDir() + "/expected/" + self.getId() + ext;

    def _getLines(self, file):
        fh = open(file)
        lines = fh.readlines()
        fh.close()
        return lines

    def mustExist(self, path):
        if not os.path.exists(path):
            self.fail("file does not exist: " + path)

    def diffExpected(self, ext):
        """diff expected and output files"""

        expFile = self.getExpectedFile(ext)
        expLines = self._getLines(expFile)

        outFile = self.getOutputFile(ext)
        outLines = self._getLines(outFile)

        diff = difflib.unified_diff(expLines, outLines, expFile, outFile)
        cnt = 0
        for l in diff:
            print(l, end=' ')
            cnt += 1
        self.failUnless(cnt == 0)

    def createOutputFile(self, ext, contents=""):
        """create an output file, filling it with contents."""
        fpath = self.getOutputFile(ext)
        fileOps.ensureFileDir(fpath)
        fh = open(fpath, "w")
        try:
            fh.write(contents)
        finally:
            fh.close()

    def verifyOutputFile(self, ext, expectContents=""):
        """verify an output file, if contents is not None, it is a string to
        compare to the expected contents of the file."""
        fpath = self.getOutputFile(ext)
        self.mustExist(fpath)
        self.failUnless(os.path.isfile(fpath))
        fh = open(fpath)
        try:
            got = fh.read()
        finally:
            fh.close()
        self.failUnlessEqual(got, expectContents)

    @staticmethod
    def numRunningThreads():
        "get the number of threads that are running"
        n = 0
        for t in threading.enumerate():
            if t.isAlive():
                n += 1
        return n

    def failIfMultipleThreads(self):
        "fail if more than one thread is running"
        self.failUnlessEqual(self.numRunningThreads(), 1)

    def failIfChildProcs(self):
        "fail if there are any running or zombie child process"
        e = None
        try:
            s = os.waitpid(0, os.WNOHANG)
        except OSError as e:
            if e.errno != errno.ECHILD:
                raise
        if e == None:
            self.fail("pending child processes or zombies: " + str(s))

    @staticmethod
    def numOpenFiles():
        "count the number of open files"
        n = 0
        for fd in xrange(0, MAXFD):
            try:
                os.fstat(fd)
            except:
                n += 1
        return MAXFD-n

    def failIfNumOpenFilesChanged(self, prevNumOpen):
        "fail if the number of open files changed"
        numOpen = self.numOpenFiles()
        if numOpen != prevNumOpen:
            self.fail("number of open files changed, was " + str(prevNumOpen) + ", now it's " + str(numOpen))

    def failUnlessMatch(self, obj, expectRe, msg=None):
        """Fail if the str(obj) does not match expectRe
           operator.
        """
        if not re.match(expectRe, str(obj), re.DOTALL):
            raise self.failureException(msg or "'%s' does not match '%s'" % (str(obj), expectRe))

    def __logCmd(self, cmd):
        cmdStrs = [quote(a) for a in cmd]
        sys.stderr.write("run: " + " ".join(cmdStrs) + "\n")

    def runProg(self, cmd, stdin=None, stdout=None, stderr=None):
        "run a program, print the command being executed"
        self.__logCmd(cmd)
        subprocess.check_call(cmd, stdin=stdin, stdout=stdout, stderr=stderr)

    def runProgOut(self, cmd, stdin=None, stderr=None):
        "run a program and return stdout, print the command being executed"
        self.__logCmd(cmd)
        p = subprocess.Popen(cmd, stdin=stdin, stdout=subprocess.PIPE, stderr = stderr)
        (stdoutText, junk) = p.communicate()
        exitCode = p.wait()
        if exitCode:
            raise subprocess.CalledProcessError(exitCode, cmd)
        return stdoutText

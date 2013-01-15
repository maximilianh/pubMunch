# Copyright 2006-2012 Mark Diekhans
"""Functions useful for debugging"""
import os.path,sys,posix


# N.B. doesn't use fileOps so it can be use in Pipeline
def _prLine(fh, *objs):
    "write each str(obj) followed by a newline"
    for o in objs:
        fh.write(str(o))
    fh.write("\n")

def lsOpen(msg=None, fh=sys.stderr, pid=None):
    """list open files, mostly for debugging"""
    if msg != None:
        _prLine(fh, msg)
    if pid == None:
        pid = os.getpid()
    fddir = "/proc/" + str(pid) + "/fd/"
    fds = posix.listdir(fddir)
    fds.sort()
    for fd in fds:
        # entry will be gone for fd dir when it was opened
        fdp = fddir + fd
        if os.path.exists(fdp):
            _prLine(fh, "    ", fd, " -> ", os.readlink(fdp))
    
__all__ = [lsOpen.__name__]

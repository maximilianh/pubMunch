# Copyright 2006-2012 Mark Diekhans
"""module that provides a wrapper around a file object that is a bit
easier to use for readline lines from ascii files"""


class LineFile(file):
    """file object oriented towards reading line-oriented files
    fields:
       - lineNum - zero-based line number of the line that was last read,
         with -1 indicating no lines have been read and None if line number
         is unknown (like after a seek)
    """
    def __init__(self, fileName):
        file.__init__(self, fileName)
        self.lineNum = -1

##FIXME: not done

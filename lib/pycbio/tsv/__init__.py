# Copyright 2006-2012 Mark Diekhans
from pycbio.tsv.TSVRow import TSVRow
from pycbio.tsv.TSVReader import TSVReader, strOrNoneType, intOrNoneType
from pycbio.tsv.TSVTable import TSVTable
from pycbio.tsv.TSVError import TSVError
# FIXME: the same module name causes confusion when doing this:
#    for row in TabFile.TabFileReader(opts.cdsFile, hashAreComments=True):
#    AttributeError: type object 'TabFile' has no attribute 'TabFileReader'
from pycbio.tsv.TabFile import TabFile
from pycbio.tsv.TabFile import TabFileReader

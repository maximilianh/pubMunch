# Copyright 2006-2012 Mark Diekhans
from pm_pycbio.tsv.TSVRow import TSVRow
from pm_pycbio.tsv.TSVReader import TSVReader, strOrNoneType, intOrNoneType
from pm_pycbio.tsv.TSVTable import TSVTable
from pm_pycbio.tsv.TSVError import TSVError
# FIXME: the same module name causes confusion when doing this:
#    for row in TabFile.TabFileReader(opts.cdsFile, hashAreComments=True):
#    AttributeError: type object 'TabFile' has no attribute 'TabFileReader'
from pm_pycbio.tsv.TabFile import TabFile
from pm_pycbio.tsv.TabFile import TabFileReader

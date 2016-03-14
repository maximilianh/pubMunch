#!/usr/bin/env python
# import Python 3.x functionality and other required modules
from __future__ import division, print_function
import sys, gzip
"""usage: convertCorpusToPubMunch.py CORPUSFILE FILESFILE ARTICLESFILE

Converts a corpus file into the pubMunch format consisting of .articles.gz and .files.gz archives. The lines in the corpus file have to have the following format: documentID<tab>documentContent.
"""

corpusFile = open(sys.argv[1], 'r')
filesFile = gzip.open(sys.argv[2], 'wb')
articlesFile = gzip.open(sys.argv[3], 'wb')
print("#fileId\texternalId\tarticleId\turl\tdesc\tfileType\ttime\tmimeType\tcontent", file=filesFile)
print("#articleId\texternalId\tsource\tpublisher\torigFile\tjournal\tprintIssn\teIssn\tjournalUniqueId\tyear\tarticleType\tarticleSection\tauthors\tauthorAffiliations\tkeywords\ttitle\tabstract\tvol\tissue\tpage\tpmid\tpmcId\tdoi\tfulltextUrl\ttime", file=articlesFile)
for line in corpusFile:
    splittedLine = line.strip().split('\t')
    documentID = splittedLine[0]
    documentContent = splittedLine[1]
    print("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}".format(documentID, documentID, documentID, '', '', 'main', '', 'text/plain', documentContent),file=filesFile)
    print("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11}\t{12}\t{13}\t{14}\t{15}\t{16}\t{17}\t{18}\t{19}\t{20}\t{21}\t{22}\t{23}\t{24}".format(documentID, documentID, 'corpus', 'corpus', '', '', '', '', '', '2013', 'corpus', '', '', '', '', '', '', '', '', '', '', '', '', '', ''), file=articlesFile)

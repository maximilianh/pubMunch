import marshal
ofh = open("counts.tsv", "w")
d = marshal.load(open("/hive/data/inside/pubs/classify/elsevier-pmc-crawler/wordCounts.marshal"))
for word, pmids in d.iteritems():
    ofh.write("%s\t%d\n" % (word, len(pmids)))
    

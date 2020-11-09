# convert scopus title list and asjc table to tab-sep file parsable by pubPublishers or pubPrepCrawl
import logging, collections, operator
def parseAsjc():
    d = {}
    for line in open("asjc_codes.txt"):
        fields = line.strip("\n").strip("\r").split("\t")
        #if len (fields)!=2:
            #print fields
        code, desc = fields[:2]
        desc = desc.strip('" ')
        d[code]=desc
    return d

topIssns = collections.defaultdict(set)
firstTopCounts = collections.Counter()

codeToDesc = parseAsjc()
print "title\tpublisher\tpIssn\teIssn\tpubCountry\tmeshTerms\tasjcTypes\turls"
fh = open("title_list.tab")
headers = None
for line in fh:
    line = line.decode("latin1").encode('utf8')
    fields = line.strip("\n").strip("\r").split("\t")
    if headers == None:
        headers = fields
        headers = [x.strip() for x in headers]
        continue
    #print len(fields), list(enumerate(fields))
    #assert(len(fields)==58)
    #print fields
    #headers = ["sourceId", "sourceTitle", "pIssn", "eIssn", "yearCov", "actInact", "snip2010", "sjr2010", "snip2011", "2011sjr", "snip2012", "sjr2012", "isMedline", "isOa", "afterApril2013", "sourceType", "titleHist", "publisher", "imprint", "country", "classCodes"]
    terms = []
    #terms.append(
    #pIssn = fields[2]
    #eIssn = fields[3]
    pIssn = fields[headers.index("Print-ISSN")]
    eIssn = fields[headers.index("E-ISSN")]

    if len(pIssn)!=0:
        pIssn = pIssn[:4]+"-"+pIssn[4:]
    if len(eIssn)!=0:
        eIssn = eIssn[:4]+"-"+eIssn[4:]

    # top level classes
    firstClassField = headers.index("Top level:  Life Sciences")
    topLevels = [t.strip(' "') for t in fields[firstClassField:] if t!=""]
    topLevelStr = "|".join(topLevels)
    if len(topLevels)>0:
        firstTopCounts[topLevels[0]]+=1

    # keep issn for top level classes
    if pIssn!="":
        for tl in topLevels:
            topIssns[tl].add(pIssn)

    # asjc codes to text
    asjcCodes = fields[headers.index("All Science Classification Codes (ASJC)")].split("; ")
    asjcCodes = [s.strip('; "\r') for s in asjcCodes if s!=""]
    asjcDescs = []
    for ac in asjcCodes:
        if ac not in codeToDesc:
            logging.warn("%s not a valid ASJC code" % ac)
        else:
            asjcDescs.append(codeToDesc[ac])
        
    asjcDesc  = "|".join(asjcDescs)

    row = [fields[1], fields[headers.index("Publisher's Name")].strip('"'), pIssn, eIssn, fields[headers.index("Publisher's Country")], topLevelStr, asjcDesc, ""]
    #row = [str(x) for x in row]
    row = [r.strip('" ') for r in row]
    print "\t".join(row)

# create table with journal counts of the first top level ASJC code
fname1 = "out.scopus.firstTopCounts.txt"
ofh = open(fname1, "w")
for desc, count in firstTopCounts.most_common():
    ofh.write("%s,%d\n" % (desc, count))
ofh.close()

# create table with ISSN counts for all top level ASJC codes
fname2 ="out.scopus.asjcTopCounts.txt"
tlc = open(fname2, "w")
topIssns = topIssns.items()
topIssnCounts = [(len(y), x) for x,y in topIssns]
topIssnCounts.sort(reverse=True)
for count, name in topIssnCounts:
    tlc.write("%s\t%d\n" % (name, count))

logging.warn("Wrote scopus summaries to %s %s" % (fname1, fname2))

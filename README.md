# Overview

These are the tools that I wrote for the UCSC Genocoding project, see
http://text.soe.ucsc.edu. They allow you to download fulltext research
articles from the internet, convert them to text and run text mining algorithms
on them.  All tools start with the prefix "pub". 
This is a early testing release, please send error messages to Maximilian Haeussler, max@soe.ucsc.edu.

# The tools

- pubCrawl = crawl papers from various publishers, needs a directory with a
        textfile "pmids.txt" in it and the data/journalList directory
- pubGetPUB = download files from publisher PUB directly (medline, pmc, elsevier)
- pubConvPUB = convert downloaded files to my pub format (tab-separated table
             with fields defined in lib/pubStore.py)
- pubLoadMysql and pubLoadSqlite = load pub format data into a database system 
- pubRunAnnot = run an annotator from the taggers directory on text data in
             pub format
- pubRunMapReduce = run a map/reduce style job from "taggers" onto fulltext.
- pubLoad = load pub format files into mysql db
- pubMap = complex multi stage pipeline to find and map markers found in text 
           (sequences, snps, bands, genes, etc) to genomic locations 
           and create/load bed files into the ucsc browser
- pubPrepX = prepare directory structures. These are used to download
        taxon names, import gene models from websites like NCBI or
        UCSC. 

If you plan to use any of these, make sure to go over lib/pubConf.py first.
Most commands need some settings in the config file adapted to your particular
server / cluster system. E.g. pubCrawl needs your email address, pubConvX 
need the cluster system (SGE or parasol) and various input/output directories.

# Common command line options

Remember that all programs mentioned here accept the -d and -v options, which will output
lots of debugging information. Many programs accept -c which specifies the cluster to use.
You can either specify the "headnode" of a cluster, so the program will ssh onto it and run
commands there. An alternative is to specify "localhost" to force running on the local machine,
or "localhost:5" to use 5 CPUs for the processing.

# An example run

Create a directory

    mkdir myCrawl

Get a list of PMIDs, put them into the file pmids.txt

    echo 17695372 > myCrawl/pmids.txt

Run the crawler in unrestricted mode and with debug output on this list: (in the default, restricted mode, it will only crawl a single publisher) 

    pubCrawl -du myCrawl

The PDFs should then be in the subdirectory myCrawl/files. Error messages are in myCrawl/pmidStatus.txt, and a crawler log file crawler.log with all sorts of status messages to help me debug problems.  Metadata (authors, title, etc) is in a sqlite database and also a tab separated file in the same directory. 

Convert crawled PDFs to text:

    mkdir myCrawlText
    pubConvCrawler myCrawl myCrawlText

This will convert html, xml, pdf, txt, ppt, doc, xls and some other file formats, if you have installed the necessary packages.

# Output format

To allow easy processing on a cluster of metadata and text separately, the tools store the text as gzipped tab-sep tables, split into *chunks* of several hundred rows each (configurable). There are two tables for each chunk:
- articles.gz
- files.gz

The table "articles" contains basic information on articles. The internal article integer ID, an "external ID" (PII for Elsevier, PMID for crawled articles, Springer IDs for Springer articles, etc), the article authors, title, abstract, keywords, DOI, year, source of the article, fulltext URL, etc (see lib/pubStore.py for all fields). The internal article identifier (articleId) is a 10 digit number and is unique across all publishers. Duplicated articles (which can happen) will have different articleIds.

The table "files" contains the files, one or more per article: the ASCII content string, the URL for each file, the date when it was downloaded, a MIME type etc. All files also have a column with the external identifier of the article associated to it. The internal fileID is the article identifier plus some additional digits. To get the article for a file, you can either use the externalID (like PMID12345) or the first 10 digits of fileId. 

One article can have several main fulltext files and several supplemental files. It should have at least one main file (even though in an old version of the tables, there were articles without any file, this should be corrected by now). 

This format allows you to use the normal UNIX textutils. E.g. to search for all articles that contain the word HOXA2 and get their external IDs (which is the PMID for crawled data) you can use simply zgrep:

    zgrep HOXA2 *.files.gz | cut -f2 | less

As the files are sorted on the articleId, you can create a big table that includes both meta information and files in one table by gunzipping all files first and then running a join:

    join 0_00000.articles 0_00000.files > textData.tab

# Annotator taggers

While you can get quite far with the UNIX tools, you might want write your text analysis as python scripts. If your scripts comply with the format required by pubRunAnnotate or pubRunMapReduce, the scripts don't have to do any parsing of the tables themselves, their output format is standardised and they get distributed over the cluster automatically.

The minimum that is required is a variable called "headers" and a function
called "annotateFile" that accepts an article object and a file object and
yields rows that are described in "headers". Here is a minimal example that searches
for the first occurence of the string " FOXO1 " and returns it together with
the year of the article:

    headers = ["start", "end", "year", "geneId"]
  
    def annotateFile(article, file):
        text = file.content
        start = text.find(" FOXO1 ")
        rows = []
        if start!=-1:
            rows.append( (start, start+8, article.year, "FOXO1") )
        return rows

Paste this code into a file called taggers/foxFinder.py, then run the command

    pubRunAnnot foxFinder.py myCrawlText --cat foxFinderOut.tab 

For a parasol cluster, the command looks like this:
    pubRunAnnot foxFinder.py myCrawlText --cat foxFinderOut.tab --cluster pk:parasol
To use 10 local CPUs, run it like this: 
    pubRunAnnot foxFinder.py myCrawlText --cat foxFinderOut.tab --cluster localhost:10

the tools will submit one cluster job for each chunk of articles. Each job will get one chunk of articles from the myCrawlText directory, parse the articles and files tables and run them through foxFinder.py. As our function yields fields called "start" and
"end", 150 characters around each FOXO1-match will be extracted and appended to
the rows as a field "snippet".

The results are written to gzipped tables with the columns articleId,
externalId, start, end, year, geneId and snippet. Since we provided the --cat
option, once the cluster jobs are done, their results will be concatenated into
one big table, foxFinderOut.tab. Depending on how big your cluster is, this can
be a lot faster than running a grep.

There is a collection of taggeres in the directory taggers/. 

These scripts can use Java classes. If the name of the script starts with "java", pubRunAnnot will run the script not in the normal python interpreter, but through Jython. That means that you can add .jar
files to sys.path in your script and use the Java classes as you would use python classes.

The taggers can set a few additional special variables, apart from "headers":
- "onlyMain": If this is set to True, the annotator will only be run on the main files, not the supplemental data.
- "onlyMeta": If True, annotator will only be run on the metadata, not the fulltext
- "preferXml": If True, annotator will prefer XML/HTML files, if both PDF and XML are available. If only PDF is available, this has no effect. Use this for highest quality text, e.g. grammatical parsers.
- "preferPdf": Like preferXml, but priority on PDF files. Use this for most comprehensive text, e.g. identifier search.

Apart from the annotateFile() function, the taggers can provide three other functions, which are loosely
inspired by Hadoop:
- "setup": function that is run before any files are opened, when the job comes up. The parameter is 
  paramDict, a dictionary with parameters specified on the command line in the format key=value.
  This can be used to read necessary data for a job.
- "startup": function that is run after the main in/out files are opened. The parameter is outFiles,
  a list of output files. This can be used to write or change headers of output files.
- "cleanup": function that is run when the job is completed. There are no parameters. This can used
  to cleanup any output from startup() or close files.

# Map/reduce operations

Sometimes you do not want to just concat results but rather
collect data from the complete text, to do something more complicated, e.g. sum values, take averages, collect word usage statistics or sentence info.  You can use map/reduce style jobs for this (see http://en.wikipedia.org/wiki/MapReduce). 

For this, you need to define (apart from the "headers" variable), two functions: map(file, article, text, resultDict) and reduce (key, valList). "resultDict" is a dictionary of key -> value. The function "map" can add (key,value) pairs to it. These results get written to files on the cluster, one per job. Once all jobs have completed, the pubRunMapReduce script calls your function "reduce" with a key and a list of all values for this key. It can yield rows for the final output file, described by the "headers" variable.

It is a lot easier to understand this with an example:
 
    headers = ["pmid", "textLen"]
    
    def map(article, file, text, resultDict):
        pmid = article.pmid
        resultDict.setdefault(pmid, 0)
        resultDict[pmid]+=len(text)
    
    def reduce(key, valList):
        pmid = key
        textSum = sum(valList)
        yield pmid, textSum

This example will first create a map with PMID -> length of text on the
cluster, then calculate the sum of all the lengths on the cluster headnode and
write the result to a tab-sep table with columns "pmid" and "textLen".

# Installation

Install these packages in ubuntu:
    sudo apt-get install catdoc poppler-utils docx2text gnumeric python-lxml

- catdoc contains various converters for Microsoft Office files
- poppler-utils contains the pdftotext converter
- docx2text is a perl script for docx files
- gnumeric includes the ssconvert tools for xslx Excel files
- python-lxml is a fast xml/html parser

If regular-expression based text annotation is too slow:
The re2 library will make it at least 10 times faster. It is a regular expression engine that avoids backtracking as far as possible, developed originally at Google. To install it, you need to download the C++ source from re2.googlecode.com, compile and install it
"make;make install" (by default to /usr/local), then install the python wrapper
with "pip install re2". (If you don't have write access to /usr/local, install the re2 library with "make install prefix=<dir>", then hack setup.py in the python re2 install package, by replacing "/usr" with your <dir>)

# BUGS to fix:

fixme: illegal DOI landing page
http://www.nature.com/doifinder/10.1046/j.1523-1747.1998.00092.x

URL constructor:
http://www.nature.com/nature/journal/v437/n7062/full/4371102a.html
for DOI  doi:10.1038/4371102a

URL construction for supplemental files:
http://www.nature.com/bjc/journal/v103/n10/suppinfo/6605908s1.html

no access page:
http://www.nature.com/nrclinonc/journal/v7/n11/full/nrclinonc.2010.119.html
- in wget, it triggers a 401 error

cat /cluster/home/max/projects/pubs/crawlDir/rupress/articleMeta.tab | head
-n13658 | tail -n2 > problem.txt

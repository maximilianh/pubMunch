# Overview

These are the tools that I wrote for the UCSC Genocoding project, see
http://text.soe.ucsc.edu. They allow you to download fulltext research
articles from internet, convert them to text and run text mining algorithms
on them.  All tools start with the prefix "pub". 
This is a early testing release, please send error messages to Maximilian Haeussler, max@soe.ucsc.edu.

# The tools

- pubCrawl = crawl papers from various publishers, needs a directory with a
        textfile "pmids.txt" in it and the data/journalList directory
- pubGet<PUB> = download files from publisher PUB directly (medline, pmc, elsevier)
- pubConv<PUB> = convert downloaded files to my pub format (tab-separated table
             with fields defined in lib/pubStore.py)
- pubLoadMysql and pubLoadSqlite = load pub format data into a database system 
- pubRunAnnot = run an annotator from the scripts directory on text data in
             pub format
- pubRunMapReduce = run a map/reduce style job from "scripts" onto fulltext.
- pubLoad = load pub format files into mysql db
- pubMap = complex multi stage pipeline to find and map markers found in text 
           (sequences, snps, bands, genes, etc) to genomic locations 
           and create/load bed files into the ucsc browser
- pubPrepX = prepare directory structures. These are used to download
        taxon names, import gene models from websites like NCBI or
        UCSC. 

Most start with the prefix "pub", the category and then the 
data source or publisher. The categories are:

If you plan to use any of these, make sure to go over lib/pubConf.py first.
Most commands need some settings in the config file adapted to your particular
server / cluster system. E.g. pubCrawl needs your email address, pubConvX 
need the cluster system and various input/output directories.

# An example run

Create a directory

    mkdir myCrawl

Get a list of PMIDs, put them into the file pmids.txt

    echo 17695372 > myCrawl/pmids.txt

Run the crawler in unrestricted mode and with debug output on this list

    pubCrawl -du myCrawl/pmids.txt

The PDFs should be in the subdirectory myCrawl/files. Error messages are in myCrawl/pmidStatus.txt. 
Metadata is in a sqlite and a tab separated file. 

Convert crawled PDFs to text:

    mkdir myCrawlText
    pubConvCrawler myCrawl myCrawlText

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

This directory contains annotators or map/reduce scripts that can be run
with pubAnnotate/pubMap/pubReduce or pubMapReduce.

some of them need data from other websites (NCBI, hugo, etc), scripts to 
generate these can be found in data/

Run the hugo annotator like this:

pubAnnotate hugoSearch.py /hive/data/inside/literature/text/pmc/
/hive/data/inside/literature/ted/hugo/

Note that annotators that start with the word "java" are run via Jython, so
they can load and call Java classes. See javaSeth.py for an example

* MAP/REDUCE example programs:

Run the DOI map/reducers like this:
../pubMap doiSearch.py /hive/data/inside/literature/text/pmc/ doiWork/
../pubReduce doiSearch.py doiWork doiList.tab

* KEEPING ANNOTATOR AND MAPPER IN THE SAME FILE:
  file: keywordSearch.py 

example commands:

# first find the number of matches per keyword
mkdir test.out
../pubMap keywordSearch.py:FilterKeywords /hive/data/inside/literature/text/pmc test.out -d keywords=EncodeCellTypes.txt maxCount=999999999999
../pubReduce keywordSearch.py:FilterKeywords test.out cellLineCounts.tab
rm -rf test.out/*
# then output the matches
../pubAnnotate keywordSearch.py:Annotate /hive/data/inside/literature/text/pmc ./test.out keywords=EncodeCellTypes.txt 

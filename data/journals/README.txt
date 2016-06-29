This directory contains two files that are created by pubPrepCrawl.

journals.tab   - list of all journals
publishers.tab - a list of all publishers and some info of their journals, created from journals.tab

also:

tandfIssns.txt - 

a list of all Taylor and Francis ISSNs. Created by
downloading http://www.tandf.co.uk/journals/tfo-resources/documents/complete-journal-list.xls
and running 
    xls2csv complete-journal-list.xls | csvToTab  | cut -f3,4 | tr '\t' '\n' |
    sort -u  | grep -v ISSN > tandfIssns.txt

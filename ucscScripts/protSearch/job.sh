#!/bin/sh
blat $1 commonWords.fa  stdout -prot -minScore=18 | pslCDnaFilter stdin -minId=1.0 -minCover=1.0 stdout | pslToPslx stdin commonWords.fa $1 $2; 

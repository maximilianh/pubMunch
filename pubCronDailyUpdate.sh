#!/bin/bash
# script for daily updates of all pub data on UCSC's hgwdev

# SETUP
#set -o errexit                        # stop on errors
export PATH=$PATH:/cluster/bin/x86_64 # for hgLoadSqlTab

# VARIABLES
PYTHON=/cluster/software/bin/python
BIN=/hive/users/max/projects/pubs/tools/
DOWNBASE=/hive/data/outside/pubs
TEXTBASE=/hive/data/inside/pubs/text
BLATBASE=/hive/data/inside/pubs/blat

MEDLINEDOWNLOADDIR=${DOWNBASE}/medline
ELSDOWNLOADDIR=${DOWNBASE}/elsevier
PMCDOWNLOADDIR=${DOWNBASE}/pmc
CRAWLDIR=/hive/data/inside/pubs/crawlDir/

MEDLINECONVDIR=${TEXTBASE}/medline
ELSCONVDIR=${TEXTBASE}/elsevier
PMCCONVDIR=${TEXTBASE}/pmc
CRAWLCONVDIR=${TEXTBASE}/crawler
BLATDIR=${BLATBASE}/elsevier
CLUSTER=swarm

JOBDIR=/hive/data/inside/pubs/cronjob_runs/`date +%m-%d-%y_%H:%M`

mkdir -p $JOBDIR
cd $JOBDIR

echo JOB DIRECTORY $JOBDIR
echo DOWNLOAD 
echo __DOWNLOADING MEDLINE__
$PYTHON $BIN/pubGetMedline $MEDLINEDOWNLOADDIR
echo __DOWNLOADING ELSEVIER__
$PYTHON $BIN/pubGetElsevier $ELSDOWNLOADDIR
echo __DOWNLOADING PUBMEDCENTRAL
$PYTHON $BIN/pubGetPmc $PMCDOWNLOADDIR

echo __CONVERT MEDLINE and update DB__ 
cd $JOBDIR; time $PYTHON $BIN/pubConvMedline $MEDLINEDOWNLOADDIR $MEDLINECONVDIR --updateDb
echo __CONVERT ELSEVIER___
cd $JOBDIR; $PYTHON $BIN/pubConvElsevier $ELSDOWNLOADDIR $ELSCONVDIR
echo __CONVERT PMC___
cd $JOBDIR; time $PYTHON $BIN/pubConvPmc $PMCDOWNLOADDIR $PMCCONVDIR 
echo __CONVERT CRAWLER___
cd $JOBDIR; time $PYTHON $BIN/pubConvCrawler $CRAWLDIR $CRAWLCONVDIR 


$PYTHON $BIN/pubCrawl $CRAWLDIR --report /cluster/home/max/public_html/mining/crawlerStatus.html


#echo BLAT AND LOAD INTO MYSQL
#ssh $CLUSTER "cd $JOBDIR; $PYTHON $BIN/pubBlat steps:all $BLATDIR -u notProcessed" && \
        #$PYTHON $BIN/pubBlat load $BLATDIR -u lastProcessed

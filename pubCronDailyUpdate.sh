#!/bin/bash
# script for daily updates of all pub data on UCSC's hgwdev

# SETUP
set -o errexit                        # stop on errors
export PATH=$PATH:/cluster/bin/x86_64 # for hgLoadSqlTab

# VARIABLES
PYTHON=/cluster/software/bin/python2.7
BIN=/hive/users/max/projects/pubs/tools/
DOWNBASE=/hive/data/outside/literature
TEXTBASE=/hive/data/inside/pubs/text
BLATBASE=/hive/data/inside/pubs/blat

MEDLINEDOWNLOADDIR=${DOWNBASE}/medline
ELSDOWNLOADDIR=${DOWNBASE}/ElsevierConsyn
PMCDOWNLOADDIR=${DOWNBASE}/PubMedCentral

MEDLINECONVDIR=${TEXTBASE}/medline
ELSCONVDIR=${TEXTBASE}/elsevier
PMCCONVDIR=${TEXTBASE}/pmc
BLATDIR=${BLATBASE}/elsevier
CLUSTER=swarm

CRAWLDIR=/cluster/home/max/projects/pubs/crawlDir
JOBDIR=/hive/data/inside/literature/cronjob_runs/`date +%m-%d-%y_%H:%M`

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
cd $JOBDIR; time $PYTHON $BIN/pubConvPmc $PMCDOWNLOADDIR $PMCCONVDIR --updateDb

$PYTHON $BIN/pubCrawl $CRAWLDIR --report /cluster/home/max/public_html/mining/crawlerStatus.html


#echo BLAT AND LOAD INTO MYSQL
#ssh $CLUSTER "cd $JOBDIR; $PYTHON $BIN/pubBlat steps:all $BLATDIR -u notProcessed" && \
        #$PYTHON $BIN/pubBlat load $BLATDIR -u lastProcessed

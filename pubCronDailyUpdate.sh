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

ELSDOWNLOADDIR=${DOWNBASE}/ElsevierConsyn
PMCDOWNLOADDIR=${DOWNBASE}/PubMedCentral
ELSCONVDIR=${TEXTBASE}/elsevier
PMCCONVDIR=${TEXTBASE}/pmc
BLATDIR=${BLATBASE}/elsevier
CLUSTER=swarm

JOBDIR=/hive/data/inside/literature/cronjob_runs/`date +%m-%d-%y_%H:%M`
mkdir -p $JOBDIR
cd $JOBDIR

echo JOB DIRECTORY $JOBDIR
echo DOWNLOAD 
echo __DOWNLOADING ELSEVIER__
$PYTHON $BIN/pubGetElsevier $ELSDOWNLOADDIR
echo __DOWNLOADING PUBMEDCENTRAL
$PYTHON $BIN/pubGetPmc $PMCDOWNLOADDIR

echo __CONVERT ELSEVIER___
cd $JOBDIR; $PYTHON $BIN/pubConvElsevier $ELSDOWNLOADDIR $ELSCONVDIR
echo __CONVERT PMC___
cd $JOBDIR; $PYTHON $BIN/pubConvPmc $PMCDOWNLOADDIR $PMCCONVDIR

#echo BLAT AND LOAD INTO MYSQL
#ssh $CLUSTER "cd $JOBDIR; $PYTHON $BIN/pubBlat steps:all $BLATDIR -u notProcessed" && \
        #$PYTHON $BIN/pubBlat load $BLATDIR -u lastProcessed

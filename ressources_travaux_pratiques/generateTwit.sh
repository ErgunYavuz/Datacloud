#!/bin/bash
#title           :generatieSchemaVEnte.sh
#description     :This script will generate a stream of twits
#author		 :Jonathan Lejeune
#date            :20181031
#==============================================================================
pwd_avant=$PWD
cd `dirname $0`
WORKING_DIRECTORY=$PWD
cd $pwd_avant


$WORKING_DIRECTORY/datagenerator/datagenerator.sh -t twit -f $WORKING_DIRECTORY/generator_configs/twit/twitgenerator.properties

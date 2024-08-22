#!/bin/bash
#title           :generatieSchemaVEnte.sh
#description     :This script will generate a hbase database
#author		 :Jonathan Lejeune
#date            :20181031
#==============================================================================
pwd_avant=$PWD
cd `dirname $0`
WORKING_DIRECTORY=$PWD
cd $pwd_avant


$WORKING_DIRECTORY/datagenerator/datagenerator.sh -t hbaseschema -f $WORKING_DIRECTORY/generator_configs/hbaseschema/stereoprixhbase.properties 

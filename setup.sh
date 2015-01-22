#!/bin/bash
set -e
TGT_DATA=/user/dln
# Lets create some directories for use later
echo 'Create problem1 directories'
hadoop fs -mkdir -p $TGT_DATA/problem1/driver
hadoop fs -mkdir -p $TGT_DATA/problem1/svm
echo 'Create problem2 directories'
hadoop fs -mkdir -p $TGT_DATA/problem2
echo 'Create problem3 directories'
hadoop fs -mkdir -p $TGT_DATA/problem3/OutGraph
hadoop fs -mkdir -p $TGT_DATA/problem3/inGraph

#!/bin/bash
set -e
SRC_DATA=/user/dsc
echo 'Capture origin airport, ariline and delay '
hadoop fs -cat $SRC_DATA/smartfly/smartfly_historic.csv | awk -F, '{ print $14","$8","$17;}' | sort | uniq | ./counter.py > historic_delays.df
#hadoop fs -cat $SRC_DATA/smartfly/smartfly_historic.csv | awk -F, '{ print $14","$8","$17;}' | sort | uniq > raw_data.dat


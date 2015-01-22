#!/bin/bash
#
# This is the pipeline for problem3 of the data science challenge 3 submitted by Doug Needham
# Logging is captured and this process is set up to run as a background process since on my cluster
# It takes a while to process all of the subgraphs
# December 2014
#
# we want to exit in case of any issue
basefile=log/problem3.log
current_time=$(date "+%Y%m%d-%H%M%S")
logfile=$basefile.$current_time
echo 'Logging to ' $logfile
echo 'Start Logging ' > $logfile 2>&1

echo 'Create Final file' >> $logfile 2>&1
echo 0,0,0 > data/final/raw_suggestion.dat
for VERTEX in $( cat data/from_vertices.dat); do
	echo $VERTEX >> $logfile 2>&1
	cat data/OutGraph/$VERTEX.data/* | tr -d '()' | ./final.py $VERTEX >> data/final/raw_suggestion.dat
done
echo 'Sort the data ' 				>> $logfile 2>&1
sort -n -r data/final/raw_suggestion.dat  > data/final/srt_suggestion.dat
echo 'Now we only want the last 2 columns' 	>> $logfile 2>&1
./srt_to_final.awk data/final/srt_suggestion.dat  > data/final/trim_suggestion.dat
echo 'To produce problem3.csv head -n 70k' 	>> $logfile 2>&1
head -n 70000 data/final/trim_suggestion.dat > problem3.csv
echo 'Problem3.csv should be in the base directory now.' 	>> $logfile 2>&1



#!/bin/bash
#
# This is the pipeline for problem3 of the data science challenge 3 submitted by Doug Needham
# Logging is captured and this process is set up to run as a background process since on my cluster
# it takes a while to process all of the subgraphs
# December 2014
#
# we want to exit in case of any issue
set -e
basefile=log/problem3.log
current_time=$(date "+%Y%m%d-%H%M%S")
logfile=$basefile.$current_time
SRC_DATA=/user/dsc
TGT_DATA=/user/dln

#
# Here we set a throttle on the driver file to allow us to test the script with smaller runs.
#
limit=$1
if [ -z "$1" ]
then
        limit=100000
fi

echo 'Logging to ' $logfile
echo 'Start Logging ' > $logfile 2>&1
echo 'This run, the driver will be limited to '$limit' lines ' >> $logfile 2>&1
echo 'Source data from '$SRC_DATA', target data to '$TGT_DATA  >> $logfile 2>&1


echo 'Reformat Winklr-network.csv' 				>> $logfile 2>&1
hadoop jar $STREAMJAR -files 'csv_to_edge.awk' \
	-input $SRC_DATA/winklr/Winklr-network.csv \
	-output $TGT_DATA/problem3/winklr-network \
	-mapper csv_to_edge.awk >> $logfile 2>&1

echo 'Reformat Winklr-topClickPairs.csv' 			>> $logfile 2>&1
hadoop jar $STREAMJAR -files 'csv_to_edge.awk' \
	-input $SRC_DATA/winklr/Winklr-topClickPairs.csv \
	-output $TGT_DATA/problem3/winklr-click-pairs \
	-mapper csv_to_edge.awk >> $logfile 2>&1


echo 'Isolate from vertices from Winklr-topClickPairs.csv' 	>> $logfile 2>&1
hadoop jar $STREAMJAR -files 'awk1.awk' \
	-input $SRC_DATA/winklr/Winklr-topClickPairs.csv \
	-output $TGT_DATA/problem3/from_vertices \
	-mapper awk1.awk \
	-reducer 'uniq' >> $logfile 2>&1

echo 'Bring the data to local disk' 				>> $logfile 2>&1
hadoop fs -getmerge $TGT_DATA/problem3/winklr-network/ \
	data/winklr-network.txt 				>> $logfile 2>&1
hadoop fs -getmerge $TGT_DATA/problem3/winklr-click-pairs/ \
	data/winklr-click-pairs.txt 				>> $logfile 2>&1
hadoop fs -getmerge $TGT_DATA/problem3/from_vertices/ \
	data/master_from_vertices.dat 				>> $logfile 2>&1
#
# This is where we throttle the run
head -n $limit data/master_from_vertices.dat > data/from_vertices.dat
#
#

echo 'Cleanup this step' 					>> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem3/winklr-network/* 		>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem3/winklr-network 		>> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem3/winklr-click-pairs/* 		>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem3/winklr-click-pairs 		>> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem3/from_vertices/* 		>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem3/from_vertices 		>> $logfile 2>&1
hdfs dfs -expunge 						>> $logfile 2>&1

echo 'Copy main files out to HDFS' 				>> $logfile 2>&1
hadoop fs -put data/winklr-network.txt $TGT_DATA/problem3/.  	>> $logfile 2>&1
hadoop fs -put data/winklr-click-pairs.txt $TGT_DATA/problem3/. >> $logfile 2>&1
hadoop fs -put data/from_vertices.dat $TGT_DATA/problem3/.  	>> $logfile 2>&1

echo 'Cleanup some files ' 					>> $logfile 2>&1
rm data/winklr-network.txt 					>> $logfile 2>&1
rm data/winklr-click-pairs.txt 					>> $logfile 2>&1

rm data/.winklr-network.txt.crc 				>> $logfile 2>&1
rm data/.winklr-click-pairs.txt.crc 				>> $logfile 2>&1
rm data/.master_from_vertices.dat.crc				>> $logfile 2>&1


echo 'Prepare individual vertex files' 				>> $logfile 2>&1
hadoop fs -cat $SRC_DATA/winklr/Winklr-topClickPairs.csv > data/tmp-topClickPairs.csv 
for VERTEX in $( cat data/from_vertices.dat); do
	echo $VERTEX >> $logfile 2>&1
	cat data/tmp-topClickPairs.csv | grep ^$VERTEX | ./csv_to_edge.awk > data/inGraph/$VERTEX.graph
done
rm data/tmp-topClickPairs.csv

echo 'Copy inGraph to HDFS' >> $logfile 2>&1
hadoop fs -copyFromLocal data/inGraph/* \
	$TGT_DATA/problem3/inGraph/.  				>> $logfile 2>&1

echo 'Run as spark for the graph analysis' 			>> $logfile 2>&1
spark-submit --executor-memory 2G \
	--class "AnalyzeGraph" \
	AnalyzeGraph/target/scala-2.10/analyzegraph_2.10-1.0.jar $TGT_DATA >> $logfile 2>&1

echo 'Get the raw data for cooking in Python' 			>> $logfile 2>&1
hadoop fs -copyToLocal $TGT_DATA/problem3/OutGraph/* \
	data/OutGraph/.  					>> $logfile 2>&1


echo 'Create Final file' >> $logfile 2>&1
echo 0,0,0 > data/final/raw_suggestion.dat
for VERTEX in $( cat data/from_vertices.dat); do
	echo $VERTEX >> $logfile 2>&1
	cat data/OutGraph/$VERTEX.data/* | tr -d '()' | ./final.py $VERTEX >> data/final/raw_suggestion.dat
done
echo 'Sort the data ' 						>> $logfile 2>&1
sort -n -r data/final/raw_suggestion.dat  > data/final/srt_suggestion.dat
echo 'Now we only want the last 2 columns' 			>> $logfile 2>&1
./srt_to_final.awk data/final/srt_suggestion.dat  > data/final/trim_suggestion.dat
echo 'To produce problem3.csv head -n 70k' 			>> $logfile 2>&1
head -n 70000 data/final/trim_suggestion.dat > problem3.csv
echo 'Problem3.csv should be in the base directory now.' 	>> $logfile 2>&1


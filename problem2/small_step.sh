#!/bin/bash
#
# This is the pipeline for problem2 of the data science challenge 3 submitted by Doug Needham
# December 2014
#

#
# we want to exit in case of any issue
set -e
basefile=log/problem2.log
current_time=$(date "+%Y%m%d-%H%M%S")
logfile=$basefile.$current_time
SRC_DATA=/user/dsc
TGT_DATA=/user/dln
echo 'Logging to ' $logfile
echo 'Start Logging ' > $logfile 2>&1


#
# Question 4
echo 'This is to capture the data for the G-Test' 		>> $logfile 2>&1
hadoop jar $STREAMJAR -Dmapreduce.job.reduces=1 \
	-files 'experiment.py,experiment_reducer.py,uid_dict.py' \
	-input $SRC_DATA/famous/web.log \
	-output $TGT_DATA/problem2/experiment_total \
	-mapper experiment_landing.py \
	-reducer experiment_reducer.py 				>> $logfile 2>&1

hadoop jar $STREAMJAR -Dmapreduce.job.reduces=1 \
	-files 'experiment.py,experiment_reducer.py,uid_dict.py' \
	-input $SRC_DATA/famous/web.log \
	-output $TGT_DATA/problem2/experiment_signup \
	-mapper experiment_signup.py \
	-reducer experiment_reducer.py 				>> $logfile 2>&1

echo 'Copy the data locally' 					>> $logfile 2>&1
hadoop fs -getmerge $TGT_DATA/problem2/experiment_total/ data/experiment_total
hadoop fs -getmerge $TGT_DATA/problem2/experiment_signup/ data/experiment_signup

echo 'Cleanup this step' 					>> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem2/experiment_total/* 		>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem2/experiment_total		>> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem2/experiment_signup/* 		>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem2/experiment_signup		>> $logfile 2>&1
hdfs dfs -expunge 						>> $logfile 2>&1

echo 'Summarize the data, and put it in JSON format' 		>> $logfile 2>&1
cat data/g_input | ./sum_exp.py > json/question4.json

#Question 5
echo 'Capture the Order data' 					>> $logfile 2>&1
hadoop jar $STREAMJAR -Dmapreduce.job.reduces=1 \
	-files 'experiment_order.py,experiment_order_reducer.py,uid_dict.py' \
	-input $SRC_DATA/famous/web.log \
	-output $TGT_DATA/problem2/order_input \
	-mapper experiment_order.py \
	-reducer experiment_order_reducer.py 			>> $logfile 2>&1

echo 'Copy the data locally' 					>> $logfile 2>&1
hadoop fs -getmerge $TGT_DATA/problem2/order_input data/order_input

echo 'Cleanup this step' 					>> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem2/order_input/* 			>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem2/order_input 		>> $logfile 2>&1
hdfs dfs -expunge 						>> $logfile 2>&1

echo 'Summarize the data, and put it in JSON format' 		>> $logfile 2>&1
cat data/order_input | ./sum_order.py > json/question5.json


# We have some individual JSON files, since we broke everything apart into small chunks.
# This is where we roll up everything, and save the problem2.json file as requested in the requirements

echo 'Run the final_output python code to generate problem2.json' >> $logfile 2>&1
./final_output.py > problem2.json

#


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

# step 0 Identify the bots
echo 'Map uid_time_action to a reducer that creates a dictionary for all UIDs that are bots' >> $logfile 2>&1
hadoop jar $STREAMJAR -Dmapreduce.job.reduces=1 \
	-files 'uid_time_action.py,bot_reducer.py' \
	-input $SRC_DATA/famous/web.log \
	-output $TGT_DATA/problem2/uid_dict \
	-mapper uid_time_action.py \
	-reducer bot_reducer.py 				>> $logfile 2>&1

#
echo 'Generate a dictionary for the uids we believe are bots' 	>> $logfile 2>&1
hadoop fs -cat $TGT_DATA/problem2/uid_dict/* | sort | ./gen_uid_dict.py > uid_dict.py
#

echo 'Cleanup this step' 					>> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem2/uid_dict/* 			>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem2/uid_dict 			>> $logfile 2>&1
hdfs dfs -expunge 						>> $logfile 2>&1


# Step 1 Identify uniq visitors whose UID is in the dictionary we just created
# This is Question 1
echo 'Identify Unique visitors that are bots' 			>> $logfile 2>&1
hadoop jar $STREAMJAR -Dmapreduce.job.reduces=1 \
	-files 'bot_uids.py,uid_dict.py' \
	-input $SRC_DATA/famous/web.log \
	-output $TGT_DATA/problem2/bot_uids \
	-mapper bot_uids.py \
	-reducer 'uniq' 					>> $logfile 2>&1
#
echo 'Bring the data to a single file locally' 			>> $logfile 2>&1
hadoop fs -getmerge $TGT_DATA/problem2/bot_uids/ data/bot_uids
#
# count the lines
cat data/bot_uids | wc -l > data/bot_uids.number
# keep it here for now, we will have a single python json generator run at the very end
#
echo 'Cleanup this step' 					>> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem2/bot_uids/* 			>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem2/bot_uids 			>> $logfile 2>&1
hdfs dfs -expunge 						>> $logfile 2>&1

# Step 1.1 Identify uniq visitors whose UID is not in the dictionary we just created
echo 'Identify Unique visitors that are bots' 			>> $logfile 2>&1
hadoop jar $STREAMJAR -Dmapreduce.job.reduces=1 \
	-files 'nonbot_visit_count.py,uid_dict.py' \
	-input $SRC_DATA/famous/web.log \
	-output $TGT_DATA/problem2/nonbot_visits \
	-mapper nonbot_visit_count.py \
	-reducer 'uniq' 					>> $logfile 2>&1
#
echo 'Bring the data to a single file locally' 			>> $logfile 2>&1
hadoop fs -getmerge $TGT_DATA/problem2/nonbot_visits/ data/nonbot_visits
#
# count the lines
cat data/nonbot_visits | wc -l > data/nonbot_visits.number
# keep it here for now, we will have a single python json generator run at the very end
#
echo 'Cleanup this step' 					>> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem2/nonbot_visits/* 		>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem2/nonbot_visits		>> $logfile 2>&1
hdfs dfs -expunge 						>> $logfile 2>&1

echo 'Capture uid count for later use' 				>> $logfile 2>&1
hadoop jar $STREAMJAR -Dmapreduce.job.reduces=1 \
	-files 'uid_count.py,uid_dict.py' \
	-input $SRC_DATA/famous/web.log \
	-output $TGT_DATA/problem2/uid_count \
	-mapper uid_count.py \
	-reducer 'uniq' 					>> $logfile 2>&1

hadoop fs -getmerge $TGT_DATA/problem2/uid_count/ data/uid_count

echo 'Cleanup this step' 					>> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem2/uid_count/* 			>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem2/uid_count 			>> $logfile 2>&1
hdfs dfs -expunge 						>> $logfile 2>&1

# We need this later. 
cat data/uid_count | wc -l > data/uid_count.number


# Question 2 Identify the adclicks: 
echo 'Identify Unique visitors' 				>> $logfile 2>&1
hadoop jar $STREAMJAR -Dmapreduce.job.reduces=1 \
	-files 'adclick_count.py,uid_dict.py' \
	-input $SRC_DATA/famous/web.log \
	-output $TGT_DATA/problem2/adclick_count \
	-mapper adclick_count.py \
	-reducer 'wc -l' 					>> $logfile 2>&1
#
echo 'Bring the data to a single file locally' 			>> $logfile 2>&1
hadoop fs -getmerge $TGT_DATA/problem2/adclick_count/ data/adclick_count.number
# keep it here for now, we will have a single python json generator run at the very end
#
echo 'Cleanup this step' 					>> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem2/adclick_count/* 		>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem2/adclick_count 		>> $logfile 2>&1
hdfs dfs -expunge 						>> $logfile 2>&1



# Question 3 
	# Part 1 identify campaigns and queries
echo 'Capture all of the campaign data' 			>> $logfile 2>&1
hadoop jar $STREAMJAR -Dmapreduce.job.reduces=1 \
	-files 'visitid_time_action_query.py,uid_dict.py' \
	-input $SRC_DATA/famous/web.log \
	-output $TGT_DATA/problem2/campaign_data \
	-mapper visitid_time_action_query.py \
	-reducer 'uniq' 					>> $logfile 2>&1

echo 'Summarize the campaigns' 					>> $logfile 2>&1
hadoop jar $STREAMJAR -files 'campaign_summary.py,simple_reducer.py' \
	-input $TGT_DATA/problem2/campaign_data \
	-output $TGT_DATA/problem2/campaign_summary \
	-mapper campaign_summary.py \
	-reducer simple_reducer.py 				>> $logfile 2>&1


hadoop fs -cat $TGT_DATA/problem2/campaign_summary/* | ./mean_reducer.py > data/meandata.json

echo 'Cleanup this step' 					>> $logfile 2>&1

hadoop fs -rm $TGT_DATA/problem2/campaign_data/* 		>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem2/campaign_data 		>> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem2/campaign_summary/* 		>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem2/campaign_summary 		>> $logfile 2>&1
hdfs dfs -expunge 						>> $logfile 2>&1

	# Part 2 Calculate standard Deviation and store it in JSON
./format_standard_dev.py > json/question3.json
#
# Question 4
echo 'This is to capture the data for the G-Test' 		>> $logfile 2>&1
hadoop jar $STREAMJAR -Dmapreduce.job.reduces=1 \
	-files 'experiment_landed.py,experiment_reducer.py,uid_dict.py' \
	-input $SRC_DATA/famous/web.log \
	-output $TGT_DATA/problem2/experiment_total \
	-mapper experiment_landed.py \
	-reducer experiment_reducer.py 				>> $logfile 2>&1

hadoop jar $STREAMJAR -Dmapreduce.job.reduces=1 \
	-files 'experiment_signup.py,experiment_reducer.py,uid_dict.py' \
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

echo 'Q4 Summarize the data, and put it in JSON format'		>> $logfile 2>&1
./sum_exp.py  data/experiment_total data/experiment_signup > json/question4.json

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


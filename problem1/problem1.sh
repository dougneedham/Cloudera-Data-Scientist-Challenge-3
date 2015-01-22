#!/bin/bash
#
# This is the pipeline for problem1 of the data science challenge 3 submitted by Doug Needham
# December 2014
#
# assumes the following HDFS structure: 
# $TGT_DATA/
# $TGT_DATA/problem1/
# $TGT_DATA/problem1/driver
# $TGT_DATA/problem1/svm

# we want to exit in case of any issue
set -e
basefile=log/problem1.log
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

echo 'Capture all airline codes' 			>> $logfile 2>&1
hadoop jar $STREAMJAR -files 'airline.awk' \
	-input $SRC_DATA/smartfly/ \
	-output $TGT_DATA/problem1/airline \
	-mapper airline.awk \
	-reducer '/usr/bin/uniq' 			>> $logfile 2>&1
#
echo 'Generate a dictionary for the airline codes' 	>> $logfile 2>&1
hadoop fs -cat $TGT_DATA/problem1/airline/* | sort | ./gen_airline_dict.py > airline_dict.py 
#
echo 'Cleanup this step' 				>> $logfile 2>&1 
hadoop fs -rm $TGT_DATA/problem1/airline/* 		>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem1/airline 		>> $logfile 2>&1
hdfs dfs -expunge 					>> $logfile 2>&1

#
echo 'Capture all airport codes' 			>> $logfile 2>&1
hadoop jar $STREAMJAR -files 'airport.awk' \
	-input $SRC_DATA/smartfly/ \
	-output $TGT_DATA/problem1/airport \
	-mapper airport.awk \
	-reducer '/usr/bin/uniq' 			>> $logfile 2>&1
#
echo 'Generate a dictionary for the airport codes' 	>> $logfile 2>&1
hadoop fs -cat $TGT_DATA/problem1/airport/* | sort | ./gen_airport_dict.py > airport_dict.py
#
echo 'Cleanup this step' >> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem1/airport/* 		>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem1/airport 		>> $logfile 2>&1
hdfs dfs -expunge >> $logfile 2>&1

#
echo 'Capture all plane models' 			>> $logfile 2>&1
hadoop jar $STREAMJAR -files 'plane.awk' \
	-input $SRC_DATA/smartfly/ \
	-output $TGT_DATA/problem1/plane \
	-mapper plane.awk \
	-reducer '/usr/bin/uniq' 			>> $logfile 2>&1
#
echo 'Generate a dictionary for the plane models' 	>> $logfile 2>&1
hadoop fs -cat $TGT_DATA/problem1/plane/* | sort | ./gen_plane_dict.py > plane_dict.py
#
echo 'Cleanup this step' 				>> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem1/plane/* 		>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem1/plane 		>> $logfile 2>&1
hdfs dfs -expunge 					>> $logfile 2>&1

#
echo 'Capture all seat configurations' 			>> $logfile 2>&1
hadoop jar $STREAMJAR -files 'seat_config.awk' \
	-input $SRC_DATA/smartfly/ \
	-output $TGT_DATA/problem1/seat_config \
	-mapper seat_config.awk \
	-reducer '/usr/bin/uniq' 			>> $logfile 2>&1
#
echo 'Generate a dictionary for the seat configurations' >> $logfile 2>&1
hadoop fs -cat $TGT_DATA/problem1/seat_config/* | sort | ./gen_seat_config_dict.py > seat_config_dict.py
#
echo 'Cleanup this step' 				>> $logfile 2>&1
hadoop fs -rm $TGT_DATA/problem1/seat_config/* 		>> $logfile 2>&1
hadoop fs -rmdir $TGT_DATA/problem1/seat_config 	>> $logfile 2>&1
hdfs dfs -expunge 					>> $logfile 2>&1

#
echo 'Now we collect the origin airports from the scheduled csv file to use as a driver for downstream processing.' >> $logfile 2>&1
hadoop fs -cat $SRC_DATA/smartfly/smartfly_scheduled.csv | ./airport.awk | sort | uniq > master_origin.dat
head -n $limit master_origin.dat > origin.dat

echo 'Prepare historic data in libsvm format' 		>> $logfile 2>&1
for AIRPORT in $( cat origin.dat); do
        hadoop jar $STREAMJAR -Dmapreduce.job.reduces=0  \
		-files 'airline_dict.py,airport_dict.py,plane_dict.py,seat_config_dict.py,Smartfly_Step1.py' \
		-input $SRC_DATA/smartfly/smartfly_historic.csv  \
		-output $TGT_DATA/problem1/$AIRPORT/sf_input \
		-mapper Smartfly_Step1.py \
		-cmdenv AIRPORT=$AIRPORT 		>> $logfile 2>&1

        hadoop jar $STREAMJAR -Dmapreduce.job.reduces=0  \
		-files 'airline_dict.py,airport_dict.py,plane_dict.py,seat_config_dict.py,Smartfly_Step2.py' \
		-input $SRC_DATA/smartfly/smartfly_scheduled.csv  \
		-output $TGT_DATA/problem1/$AIRPORT/sf_predict \
		-mapper Smartfly_Step2.py \
		-cmdenv AIRPORT=$AIRPORT 		>> $logfile 2>&1
	
	mkdir -p data/$AIRPORT
	hadoop fs -getmerge $TGT_DATA/problem1/$AIRPORT/sf_input/ data/$AIRPORT/svm_input
	hadoop fs -getmerge $TGT_DATA/problem1/$AIRPORT/sf_predict/ data/$AIRPORT/svm_predict
	hadoop fs -put data/$AIRPORT/svm_predict $TGT_DATA/problem1/svm/$AIRPORT.svm_predict
	hadoop fs -put data/$AIRPORT/svm_input $TGT_DATA/problem1/svm/$AIRPORT.svm_input
	hadoop fs -rm $TGT_DATA/problem1/$AIRPORT/sf_input/* 	>> $logfile 2>&1
	hadoop fs -rmdir $TGT_DATA/problem1/$AIRPORT/sf_input 	>> $logfile 2>&1
	hadoop fs -rm $TGT_DATA/problem1/$AIRPORT/sf_predict/* 	>> $logfile 2>&1
	hadoop fs -rmdir $TGT_DATA/problem1/$AIRPORT/sf_predict >> $logfile 2>&1
	hadoop fs -rmdir $TGT_DATA/problem1/$AIRPORT 		>> $logfile 2>&1
	hdfs dfs -expunge 					>> $logfile 2>&1
done


# Now we do the spark stuff
hadoop fs -put origin.dat  $TGT_DATA/problem1/driver/.
spark-submit --executor-memory 2G \
	--class "PredictFlights" \
	PredictFlights/target/scala-2.10/predictflights_2.10-1.0.jar $TGT_DATA >> $logfile 2>&1
echo 'Get the file to a local file' 			>> $logfile 2>&1
hadoop fs -getmerge $TGT_DATA/problem1/Flight_List/ data/problem1.raw

echo 'Format problem1.csv' 				>> $logfile 2>&1
cat data/problem1.raw | ./column2.awk > problem1.csv


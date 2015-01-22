## This is the submission package for Doug Needham
## Data Science Challenge 3

The proper write up for this solution is in this directory as Doug_Needham_DSC3_Write_Up.pdf 

The assumptions for this code is that it will run under the userid "dln"
The HDFS directory structure required is:
* /user/dln/problem1
* /user/dln/problem1/driver
* /user/dln/problem1/svm
* /user/dln/problem2
* /user/dln/problem3
* /user/dln/problem3/inGraph
* /user/dln/problem3/OutGraph

The shell script "setup.sh" performs the appropriate hadoop fs -mkdir -p commands to create the directories. 

As to the source data. 
All of the code that follows assumes the data for the challenge is in the following location and structure:
* /user/dsc/famous/spam.log
* /user/dsc/famous/web.log
* /user/dsc/winklr/Winklr-network.csv
* /user/dsc/winklr/Winklr-topClickPairs.csv
* /user/dsc/smartfly/smartfly_historic.csv
* /user/dsc/smartfly/smartfly_scheduled.csv

Both of the previous assumptions are used to set these environment variables in the individual shell scripts: 
- SRC_DATA=/user/dsc
- TGT_DATA=/user/dln

The three requested deliverables are under the directory named "answer", these are the "master" answers and no automation is used to copy the files from the individual code directories to the answer directory: 

- answer/
- answer/problem1.csv
- answer/problem2.json
- answer/problem3.csv

The structure of the directories for the code is as follows (The output directories created by sbt are eliminated for brevity) : 

- answer
- problem1
..*  analysis
..*  data
..*  log
..*  PredictFlights
- problem2
..-  data
..-  json
..-  log
- problem3
..-  AnalyzeGraph
..-  data
..- final
..-  inGraph
..-  OutGraph
..- log

The shell script to run each problem is in the individual problem directory.
- problem1/problem1.sh
- problem2/problem2.sh
- problem3/problem3.sh

These can all be run as a background process using problem1.sh & for example, since logging within the shell script is being done to the log directory.

problem1.sh and problem3.sh can be run with a single command line argument. Both of these scripts are data driven, in that they each have a file that drives the process. In the case of problem1, it is a list of airports, in problem3 it is a list of originating vertices. The command line argument "throttles" the proces to only run a certain number of airports, or from-vertices for problems 1 and 3 respectively.


Thank you, 

Doug Needham

dougthedataguy@gmail.com



#!/usr/bin/env bash

# get into the current directory
cd "$( cd "$( dirname "$0"  )" && pwd  )"

##### The Parameters You Need to Predefine Start #####

# set the environment variables
HADOOP_HOME="/path/to/hadoop/home"  #Hadoop home
SPARK_HOME="/path/to/spark/home"  #Spark home
topwords_jar="../release/topwords-1.0.jar"  #topwords jar file

# set the arguments
inputLoc="/path/to/corpus/in/hdfs/*"  #location of input corpus in HDFS
inputFormat="text"  #format of input files
outputLoc="/path/to/output/in/hdfs" #location of output dictionary and segmented corpus in HDFS
tauL="10"  #threshold of word length
tauF="5"  #threshold of word frequency
numIterations="10"  #number of iterations
convergeTol="1E-3"  #convergence tolerance
textLenThld="2000"  #preprocessing threshold of text length
useProbThld="1E-8"  #prune threshold of word use probability
wordBoundaryThld="0.0"  #segment threshold of word boundary score (use segment tree if set to <= 0)
numPartitions="100"  #number of partitions
executor_memory="2G"  #memory allocation for each executor
num_executors="10"  #number of executors allocated
executor_cores="2"  #number of cores allocated for each executor
queue="queue_name"  #yarn queue

##### The Parameters You Need to Predefine End #####


# execute the TopWORDS algorithm

function_exec(){
${SPARK_HOME}/bin/spark-submit \
--class io.github.qf6101.topwords.TopWORDSApp \
--master yarn-cluster \
--name topwords \
--executor-memory $executor_memory \
--num-executors $num_executors \
--executor-cores $executor_cores \
--queue $queue \
${topwords_jar} \
--inputLoc $inputLoc \
--inputFormat $inputFormat \
--outputLoc $outputLoc \
--tauL $tauL \
--tauF $tauF \
--numIterations $numIterations \
--convergeTol $convergeTol \
--textLenThld $textLenThld \
--useProbThld $useProbThld \
--wordBoundaryThld $wordBoundaryThld \
--numPartitions $numPartitions
}

output="topwords.running"
function_exec > ${output} 2>&1

sleep 60s
app_id=`grep -Eo "application_[0-9]+_[0-9]+" ${output} | head -n1`
logfile=${output}.log
${HADOOP_HOME}/bin/yarn logs -applicationId ${app_id} > ${logfile}

#check the result state
if grep -i ERROR ${logfile}
then
        rm $output
        echo "Exception occurred in $0. See `readlink -f ${logfile}`"
        exit 1
else
        rm $output
        echo "Finish $0."
        exit 0
fi

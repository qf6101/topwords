#!/usr/bin/env bash

# get into the current directory
cd "$( cd "$( dirname "$0"  )" && pwd  )"

##### The Parameters You Need to Predefine Start #####

# set the environment variables
SPARK_HOME="/home/qfeng/install/spark"  #Spark home
topwords_jar="../release/topwords-1.0.jar"  #topwords jar file

# set the arguments
inputLoc="../../test_data/story_of_stone.txt"  #location of input corpus
inputFormat="text"  #format of input files
outputLoc="../../test_data/test_output" #location of output dictionary and segmented corpus
tauL="10"  #threshold of word length
tauF="5"  #threshold of word frequency
numIterations="10"  #number of iterations
convergeTol="1E-3"  #convergence tolerance
textLenThld="2000"  #preprocessing threshold of text length
useProbThld="1E-8"  #prune threshold of word use probability
wordBoundaryThld="0.0"  #segment threshold of word boundary score (use segment tree if set to <= 0)
numThreads="1"  #number of threads

##### The Parameters You Need to Predefine End #####

# execute the TopWORDS algorithm
${SPARK_HOME}/bin/spark-submit \
--class io.github.qf6101.topwords.TopWORDSApp \
--master "local[${numThreads}]" \
--name topwords_local \
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
--wordBoundaryThld $wordBoundaryThld

#check the result state
if [ $? -ne 0 ]
then
	echo "Running TopWORDS fail at `date`"
else
	echo "Running TopWORDS successfully at `date`."
fi

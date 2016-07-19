# TopWORDS

## Brief Description

This project is an implementation of TopWORDS algorithm proposed in the following paper.

> Deng K, Bol P K, Li K J, et al. On the unsupervised analysis of domain-specific Chinese texts[J]. Proceedings of the National Academy of Sciences, 2016: 201516510.

TopWORDS can achieve word discovery and text segmentation simultaneously for Chinese texts. It is designed to be fast and use very little memory. In my test, it takes around 5 minutes to segment "The Story of Stone" with an Intel i3-4160 CPU and less than 2G memory. This implementation is based on Spark 1.6.x which means it can be used in both local machine with specified number of threads and in yarn clusters for large amount of texts.

For more information about its theory, refer to http://qf6101.github.io/machine%20learning/2016/07/01/TopWORDS (in Chinese)

## Local Machine Mode

1. Download Spark 1.6.x from http://spark.apache.org/downloads.html

2. Set the parameters in "deploy/sbin/topwords_local.sh" (simply set only SPARK_HOME if you just need to run "The
Story of Stone" example)

3. Run the script: bash deploy/sbin/topwords_local.sh

## Yarn Cluster Mode

1. Set the parameters in "deploy/sbin/topwords_yarn.sh"

2. Run th script: bash deploy/sbin/topwords_yarn.sh (you may need to initialize the keytab in advance)

## API Usage

Please refer to [src/test/scala/io/github/qf6101/topwords/TestTopWORDS.scala](src/test/scala/io/github/qf6101/topwords/TestTopWORDS.scala)

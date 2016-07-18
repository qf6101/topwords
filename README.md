# TopWORDS

## Brief Description

This project is an implementation of TopWORDS algorithm proposed in the following paper.

> Deng K, Bol P K, Li K J, et al. On the unsupervised analysis of domain-specific Chinese texts[J]. Proceedings of the National Academy of Sciences, 2016: 201516510.

TopWORDS can achieve word discovery and text segmentation simultaneously for Chinese texts. It excutes very fast (e.g., takes only 2 minutes for 《The Story of Stone》) with very few memory. My implementation uses Scala on Spark1.6.2. Hence it can be used in local machine with specified number of threads as well as in yarn clusters for large amount of texts.

## Usage

Please refer to src/test/scala/io/github/qf6101/topwords/TestTopWORDS.scala

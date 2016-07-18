package io.github.qf6101.topwords

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by qfeng on 16-7-6.
  */
object TestTopWORDS extends Serializable {
  /**
    * Test TopWORDS on《story of stone (红楼梦)》
    */
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[1]").setAppName(this.getClass.toString)
    val sc = new SparkContext(conf)
    val inputFile = "test_data/story_of_stone.txt"
    val outputFile = "test_data/test_output"
    val files = FileSystem.get(sc.hadoopConfiguration)
    if (files.exists(new Path(outputFile))) files.delete(new Path(outputFile), true)
    val corpus = sc.textFile(inputFile)
    new TopWORDS(
      tauL = 10,
      tauF = 5,
      textLenThld = 2000,
      useProbThld = 1E-8,
      numIteration = 10,
      convergeTol = 1E-3,
      wordBoundaryThld = 0.0)
      .run(corpus, outputFile + "/dictionary", outputFile + "/segmented_texts")
  }
}

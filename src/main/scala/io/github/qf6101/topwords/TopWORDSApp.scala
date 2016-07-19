package io.github.qf6101.topwords

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by qfeng on 16-7-18.
  */
object TopWORDSApp extends Logging with Serializable {
  def main(args: Array[String]) {
    try {
      TopWORDSParser.parse(args).foreach { args =>
        // setup spark context
        val conf = new SparkConf().setAppName(this.getClass.toString)
        val sc = new SparkContext(conf)
        // remove output location files if exist
        val files = FileSystem.get(sc.hadoopConfiguration)
        if (files.exists(new Path(args.outputLoc))) files.delete(new Path(args.outputLoc), true)
        // read input corpus
        val corpus = sc.textFile(args.corpusLoc)
        if (args.numPartitions > 0) corpus.repartition(args.numPartitions)
        // run TopWORDS with the parsed arguments
        new TopWORDS(
          tauL = args.tauL,
          tauF = args.tauF,
          textLenThld = args.textLenThld,
          useProbThld = args.useProbThld,
          numIterations = args.numIterations,
          convergeTol = args.convergeTol,
          wordBoundaryThld = args.wordBoundaryThld)
          .run(corpus, args.outputLoc + "/dictionary", args.outputLoc + "/segmented_texts")
      }
      //exit normally
      logInfo("Running TopWORDS successfully!")
      sys.exit(0)
    } catch {
      case ex: Throwable =>
        logError("Running TopWORDS fail!", ex)
        //signal to external process
        sys.exit(1)
    }
  }
}
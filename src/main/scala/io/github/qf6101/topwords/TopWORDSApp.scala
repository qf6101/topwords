package io.github.qf6101.topwords

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Created by qfeng on 16-7-18.
  */
object TopWORDSApp extends Serializable {
  @transient private[this] val LOGGER = Logger.getLogger(this.getClass.toString)

  def main(args: Array[String]) {
    // setup spark session
    val spark = SparkSession.builder().getOrCreate()
    try {
      TopWORDSParser.parse(args).foreach { args =>
        // remove output location files if exist
        val files = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        if (files.exists(new Path(args.outputLoc))) files.delete(new Path(args.outputLoc), true)
        // read input corpus
        val corpus = if (args.numPartitions > 0)
          spark.sparkContext.textFile(args.inputLoc).repartition(args.numPartitions)
        else spark.sparkContext.textFile(args.inputLoc)
        LOGGER.info("Number of lines of input corpus: " + corpus.count())
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
      LOGGER.info("Running TopWORDS successfully!")
      if (spark.sparkContext.master.contains("local")) sys.exit(0)
    } catch {
      case ex: Throwable =>
        LOGGER.error("Running TopWORDS fail!", ex)
        //signal to external process
        if (spark.sparkContext.master.contains("local")) sys.exit(1)
    } finally spark.stop()
  }
}
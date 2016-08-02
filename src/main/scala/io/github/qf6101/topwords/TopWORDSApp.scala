package io.github.qf6101.topwords

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by qfeng on 16-7-18.
  */
object TopWORDSApp extends Serializable {
  @transient private[this] val LOGGER = Logger.getLogger(TopWORDSApp.getClass.toString)
  def main(args: Array[String]) {
    // setup spark session
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    try {
      TopWORDSParser.parse(args).foreach { args =>
        // remove output location files if exist
        val files = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        if (files.exists(new Path(args.outputLoc))) files.delete(new Path(args.outputLoc), true)
        // read input corpus
        val corpus = spark.read.format(args.inputFormat).load(args.inputLoc).map(_.toString())
        if(args.numIterations > 0) corpus.repartition(args.numIterations)
        // run TopWORDS with the parsed arguments
        new TopWORDS(
          tauL = args.tauL,
          tauF = args.tauF,
          textLenThld = args.textLenThld,
          useProbThld = args.useProbThld,
          numIterations = args.numIterations,
          convergeTol = args.convergeTol,
          wordBoundaryThld = args.wordBoundaryThld)
          .run(corpus.persist(StorageLevel.MEMORY_AND_DISK_SER_2), args.outputLoc + "/dictionary", args.outputLoc + "/segmented_texts")
      }
      //exit normally
      LOGGER.info("Running TopWORDS successfully!")
       sys.exit(0)
    } catch {
      case ex: Throwable =>
        LOGGER.error("Running TopWORDS fail!", ex)
        //signal to external process
        sys.exit(1)
    }
  }
}
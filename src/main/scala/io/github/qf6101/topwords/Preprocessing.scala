package io.github.qf6101.topwords

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by qfeng on 16-7-6.
  */

/**
  * Preprocessing method of corpus
  *
  * @param textLenThld theshold of text length
  */
class Preprocessing(private val textLenThld: Int) extends Serializable {
  /**
    * Preprocessing method of corpus
    *
    * @param corpus corpus
    * @return preprocessed corpus
    */
  def run(corpus: Dataset[String]): Dataset[String] = {
    // importing spark implicits
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    corpus.flatMap { T =>
      // split the paragraph into several texts using punctuations and spaces
      T.split("\\pP|\\pS|\\s|　").map(_.trim)
    }.filter(_.length > 1).flatMap { T =>
      // split text according to text length threshold
      if (T.length > textLenThld) {
        var splits = List[StringBuilder]() ::: List(new StringBuilder())
        T.foreach { c =>
          if ((splits.last += c).length >= textLenThld) {
            splits = splits ::: List(new StringBuilder())
          }
        }
        // return split texts
        splits.map(_.toString())
      } else List(T)
    }
  }
}

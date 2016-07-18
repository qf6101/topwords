package io.github.qf6101.topwords

import scala.collection.mutable

/**
  * Created by qfeng on 16-7-16.
  */
/**
  * (Simple) Text Segementor
  * the result segments may not be a word in dictionary
  *
  * @param T                text to be segmented
  * @param boundaryScores   word boundary scores with regard to T's positions
  * @param wordBoundaryThld word boundary threshold
  * @param splitter         splitter (default is '|')
  */
class TextSegmentor(private val T: String,
                    private val boundaryScores: Array[Double],
                    private val wordBoundaryThld: Double,
                    private val splitter: Char = '|'
                   ) extends Serializable {
  /**
    * Segment the text in positions whose boundary score greater than the threshold
    *
    * @return the split text
    */
  def toText(): String = {
    // filter the split positions whose boundary score greater than the threshold
    val splitPositions = boundaryScores.zipWithIndex.flatMap { case (score, idx) =>
      if (score >= wordBoundaryThld) List(idx + 1) else Nil
    }.toList
    // segment the text using splitter
    segment(splitPositions)
  }

  /**
    * Inserting the splitter in the split positions
    *
    * @param splitPositions split positions
    * @return text with the splitters in the split positions
    */
  protected def segment(splitPositions: List[Int]): String = {
    // return text itself if it has only one character
    if (T.length <= 1 || splitPositions.length == 0) return T
    // copy the characters one by one plus the splitters in the boundary positions
    val splitPosStack = mutable.Stack().pushAll(splitPositions.reverse)
    var currSplitPos = splitPosStack.pop() - 1
    val splitResult = new StringBuilder()
    T.zipWithIndex.foreach { case (c, idx) =>
      splitResult += c
      if (idx == currSplitPos) {
        splitResult += splitter
        currSplitPos = if (splitPosStack.nonEmpty) splitPosStack.pop() - 1 else -1
      }
    }
    splitResult.toString()
  }
}

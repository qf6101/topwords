package io.github.qf6101.topwords

/**
  * Created by qfeng on 16-7-14.
  */

/**
  * (Complete) Segment Tree
  * the result segments must be a word in dictionary
  *
  * @param T              text to be segmented
  * @param boundaryScores word boundary scores with regard to T's positions
  * @param dict           dictionary
  * @param tauL           threshold of word length
  * @param splitter       splitter (default is '|')
  */
class SegmentTree(private val T: String,
                  private val boundaryScores: Array[Double],
                  private val dict: Dictionary,
                  private val tauL: Int,
                  private val splitter: Char = '|'
                 ) extends TextSegmentor(T, boundaryScores, splitter) {
  private val topNode = new SegTreeNode(0, T.length)

  /**
    * Segment recursively the text in positions with the maximum boundary scores
    * until the result segment is a word in dictionary
    *
    * @return the split text
    */
  override def toText(): String = {
    segment(topNode.splitPositions)
  }

  /**
    * Node of segment tree
    *
    * @param left  left child node
    * @param right right child node
    */
  private class SegTreeNode(private val left: Int,
                            private val right: Int) {
    // children nodes
    private var leftChild: Option[SegTreeNode] = None
    private var rightChild: Option[SegTreeNode] = None
    // split position
    private var splitPos: Int = -1
    // split T[left, right) as regard to maximum boundary score
    if (right - left > tauL || (right - left <= tauL && !dict.contains(T.substring(left, right)))) {
      // search split position
      var max = Double.MinValue
      for (i <- left + 1 until right) {
        if (max < boundaryScores(i - 1)) {
          max = boundaryScores(i - 1)
          splitPos = i
        }
      }
      // recursively create children nodes
      if (splitPos - left > 1) leftChild = Some(new SegTreeNode(left, splitPos))
      if (right - splitPos > 1) rightChild = Some(new SegTreeNode(splitPos, right))
    }

    /**
      * Get recursively the split positions with the pre-order traversal
      *
      * @return
      */
    def splitPositions: List[Int] = {
      val leftPositions = leftChild match {
        case Some(node) =>
          node.splitPositions
        case _ =>
          Nil
      }
      val rightPositions = rightChild match {
        case Some(node) =>
          node.splitPositions
        case _ =>
          Nil
      }
      leftPositions ::: (if (splitPos < 0) Nil else List(splitPos)) ::: rightPositions
    }
  }

}


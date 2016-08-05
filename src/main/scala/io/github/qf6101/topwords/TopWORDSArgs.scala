package io.github.qf6101.topwords

/**
  * Created by qfeng on 16-7-18.
  */
/**
  * TopWORDS Application Arguments
  *
  * @param inputLoc         location of input corpus
  * @param outputLoc        location of output dictionary and segmented corpus
  * @param tauL             threshold of word length
  * @param tauF             threshold of word frequency
  * @param numIterations    number of iterations
  * @param convergeTol      convergence tolerance
  * @param textLenThld      preprocessing threshold of text length
  * @param useProbThld      prune threshold of word use probability
  * @param wordBoundaryThld segment threshold of word boundary score (use segment tree if set to <= 0)
  * @param numPartitions    number of partitions in yarn mode
  */
case class TopWORDSArgs(inputLoc: String = "",
                        outputLoc: String = "",
                        tauL: Int = 10,
                        tauF: Int = 5,
                        numIterations: Int = 5,
                        convergeTol: Double = 1E-3,
                        textLenThld: Int = 5000,
                        useProbThld: Double = 1E-8,
                        wordBoundaryThld: Double = 0.0,
                        numPartitions: Int = -1) extends Serializable

/**
  * TopWORDS Argument Parser
  */
object TopWORDSParser extends Serializable {
  val parser = new scopt.OptionParser[TopWORDSArgs]("TopWORDSArgs") {
    head("scopt", "3.x")
    opt[String]("inputLoc") action { (x, c) =>
      c.copy(inputLoc = x)
    } text "location of input corpus"
    opt[String]("outputLoc") action { (x, c) =>
      c.copy(outputLoc = x)
    } text "location of outputs"
    opt[Int]("tauL") action { (x, c) =>
      c.copy(tauL = x)
    } text "threshold of word length"
    opt[Int]("tauF") action { (x, c) =>
      c.copy(tauF = x)
    } text "threshold of word frequency"
    opt[Int]("numIterations") action { (x, c) =>
      c.copy(numIterations = x)
    } text "number of iterations"
    opt[Double]("convergeTol") action { (x, c) =>
      c.copy(convergeTol = x)
    } text "convergence tolerance"
    opt[Int]("textLenThld") action { (x, c) =>
      c.copy(textLenThld = x)
    } text "preprocessing threshold of text length"
    opt[Double]("useProbThld") action { (x, c) =>
      c.copy(useProbThld = x)
    } text "prune threshold of word use probability"
    opt[Double]("wordBoundaryThld") action { (x, c) =>
      c.copy(wordBoundaryThld = x)
    } text "segment threshold of word boundary score (use segment tree if set to <= 0)"
    opt[Int]("numPartitions") action { (x, c) =>
      c.copy(numPartitions = x)
    } text "number of partitions in yarn mode"
  }

  def parse(args: Array[String]): Option[TopWORDSArgs] = {
    parser.parse(args, TopWORDSArgs())
  }
}
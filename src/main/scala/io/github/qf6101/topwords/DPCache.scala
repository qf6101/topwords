package io.github.qf6101.topwords

/**
  * Created by qfeng on 16-7-11.
  */

/**
  * Dynamic programming cache used to store the niTs (word use probabilities) and riTs (word significance factors)
  *
  * @param tauL          threshold of word length
  * @param incrementProc process of incrementing the value from m to m+1 (different for niTs and riTs)
  */
class DPCache(private val tauL: Int,
              private val incrementProc: (Double) => Double
             ) extends Serializable {
  private val cache = scala.collection.mutable.Map[String, LimitStack]()

  /**
    * Push the mth cuttings
    *
    * @param cuttings all possible cuttings for T_m with one word in head and rest in tail
    * @return this
    */
  def push(cuttings: Array[(String, Int, Double)]): this.type = {
    // add words to cache if they first appear in our view
    // this means their niT_[>m]S are zero
    cuttings.foreach { case (candidateWord, _, _) =>
      // add word and initialize its niT_[>m]S to zero
      if (!cache.contains(candidateWord)) cache += candidateWord -> new LimitStack(tauL, 0.0)
    }
    // push niT_m
    cache.foreach { case (word, stack) =>
      val pushValue = cuttings.foldLeft(0.0) { case (sum, (candidateWord, t, rho)) =>
        //different increment method for niTs and riTs w.r.t incrementProc
        if (word.equals(candidateWord)) sum + rho * incrementProc(stack.get(t - 1))
        else sum + rho * stack.get(t - 1)
      }
      stack.push(pushValue)
    }
    this
  }

  /**
    * Get the top value in DP cache (we usually get the T_0's DP result)
    *
    * @return top value of DP cache
    */
  def top: Map[String, Double] = {
    cache.map { case (word, stack) =>
      word -> stack.top
    }.toMap
  }
}

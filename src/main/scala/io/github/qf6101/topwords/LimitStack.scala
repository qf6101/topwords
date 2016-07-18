package io.github.qf6101.topwords

/**
  * Created by qfeng on 16-7-11.
  */
/**
  * Array implementation of stack with limit size
  *
  * @param size      limit size of stack
  * @param initValue initial value of stack elements
  */
class LimitStack(private val size: Int,
                 private val initValue: Double
                ) extends Serializable {
  require(size > 0)
  private val stack = Array.fill[Double](size)(initValue)
  private var head = size - 1

  /**
    * Push new element to stack
    *
    * @param element new element
    * @return this
    */
  def push(element: Double): this.type = {
    head = if (head + 1 >= size) 0 else head + 1
    stack(head) = element
    this
  }

  /**
    * Get the top element of stack
    *
    * @return top element of stack
    */
  def top: Double = {
    get(0)
  }

  /**
    * Get the element of certain index (index 0 starts from the last pushed element)
    *
    * @param idx the index of query element
    * @return query element
    */
  def get(idx: Int): Double = {
    require(idx >= 0 && idx < size)
    val pos = if (head - idx < 0) size + head - idx else head - idx
    stack(pos)
  }
}

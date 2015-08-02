package pl.edu.pw.elka.cnv.utils

/**
 * Created by mariusz-macbook on 26/04/15.
 *
 * Trait for doing statistical analysis.
 */
trait StatUtils {

  /**
   * Method for mean calculation.
   *
   * @param data Sequence of double values.
   * @return Calculated mean value.
   */
  def mean(data: Seq[Double]): Double =
    data.sum / data.size

  /**
   * Method for stddev calculation.
   *
   * @param data Sequence of double values.
   * @return Calculated stddev value.
   */
  def stddev(data: Seq[Double]): Double = {
    val m = mean(data)
    math.sqrt(data.map(x => math.pow(x - m, 2)).sum / data.size)
  }

  /**
   * Method for median calculation.
   *
   * @param data Sequence of double values.
   * @return Calculated median value.
   */
  def median(data: Seq[Double]): Double = {
    val (lower, upper) = data.sorted.splitAt(data.size / 2)
    if (data.size % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }

}

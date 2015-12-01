package pl.edu.pw.elka.cnv.utils

/**
 * Object containing methods for statistical analysis.
 */
object StatUtils {

  /**
   * Method for mean calculation.
   *
   * @param data Array of double values.
   * @return Calculated mean value.
   */
  def mean(data: Array[Double]): Double =
    data.sum / data.size

  /**
   * Method for stddev calculation.
   *
   * @param data Array of double values.
   * @return Calculated stddev value.
   */
  def stddev(data: Array[Double]): Double = {
    val m = mean(data)
    math.sqrt(data.map(x => math.pow(x - m, 2)).sum / data.size)
  }

  /**
   * Method for median calculation.
   *
   * @param data Array of double values.
   * @return Calculated median value.
   */
  def median(data: Array[Double]): Double = {
    val (lower, upper) = data.sorted.splitAt(data.size / 2)
    if (data.size % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }

}

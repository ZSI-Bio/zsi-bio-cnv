package pl.edu.pw.elka.cnv.utils

/**
 * Created by mariusz-macbook on 26/04/15.
 *
 * Trait for doing statistical analysis.
 */
trait StatUtils {

  /**
   * Method for stddev calculation.
   *
   * @param data Sequence of double values.
   * @return Calculated stddev value.
   */
  def stddev(data: Seq[Double]): Double = {
    val mean = data.sum / data.size
    math.sqrt(data.map(x => math.pow(x - mean, 2)).sum / data.size)
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

  /**
   * Method for RPKM calculation.
   *
   * @param count Coverage of given region by reads from given sample.
   * @param len Length of given region.
   * @param total Total number of reads in given sample.
   * @return Calculated RPKM value for given region and sample.
   */
  def rpkm(count: Int, len: Int, total: Long): Double =
    return (1000000000d * count) / (len * total)

  /**
   * Method for ZRPKM calculation.
   *
   * @param rpkm RPKM value for given region and sample.
   * @param median Median of RPKM values for given region.
   * @param stddev Stddev of RPKM values for given region.
   * @return Calculated ZRPKM value for given region and sample.
   */
  def zrpkm(rpkm: Double, median: Double, stddev: Double): Double =
    return (rpkm - median) / stddev

}

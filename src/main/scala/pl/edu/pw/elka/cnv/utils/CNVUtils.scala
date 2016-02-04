package pl.edu.pw.elka.cnv.utils

/**
 * Object containing methods for CNV detection.
 */
object CNVUtils {

  /**
   * Method for RPKM calculation.
   *
   * @param count Coverage of given region by reads from given sample.
   * @param len Length of given region.
   * @param total Total number of reads in given sample.
   * @return Calculated RPKM value for given region and sample.
   */
  def rpkm(count: Int, len: Int, total: Long): Double =
    (1000000000d * count) / (len * total)

  /**
   * Method for ZRPKM calculation.
   *
   * @param rpkm RPKM value for given region and sample.
   * @param median Median of RPKM values for given region.
   * @param stddev Stddev of RPKM values for given region.
   * @return Calculated ZRPKM value for given region and sample.
   */
  def zrpkm(rpkm: Double, median: Double, stddev: Double): Double =
    (rpkm - median) / stddev

  /**
   * Method for calculation of Blackman window.
   *
   * @param window Number of points in the output window.
   * @return Blackman window.
   */
  def blackman(window: Int): Array[Double] = {
    val d = 2 * math.Pi / (window - 1)
    (0 until window) map {
      n => 0.42 - 0.5 * math.cos(n * d) + 0.08 * math.cos(2 * n * d)
    } toArray
  }

  /**
   * Method for calculation of discrete, linear convolution of two one-dimensional arrays.
   *
   * @param v1 First one-dimensional input array.
   * @param v2 Second one-dimensional input array.
   * @return Convolution of given arrays.
   */
  def convolve(v1: Array[Double], v2: Array[Double]): Array[Double] = {
    val (n1, n2) = (v1.size, v2.size)
    (0 until n1 + n2 - 1) map { n =>
      val kMin = math.max(0, n - n2 + 1)
      val kMax = math.min(n1 - 1, n)
      (kMin to kMax).map(k => v1(k) * v2(n - k)).sum
    } toArray
  }

}

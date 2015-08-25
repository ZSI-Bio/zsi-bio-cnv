package pl.edu.pw.elka.cnv.utils

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}

/**
 * Created by mariusz-macbook on 29/07/15.
 *
 * Trait containing common CNV detection methods.
 */
trait CNVUtils {

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

  //TODO
  def smooth(matrix: IndexedRowMatrix, window: Int): IndexedRowMatrix =
    if (window <= 0) matrix
    else {
      val newRows = for {
        row <- transpose(matrix).rows
        weightings = blackman(window)
        smoothed = convolve(row.vector.toArray, weightings)
      } yield new IndexedRow(row.index, Vectors.dense(smoothed.toArray))
      transpose(new IndexedRowMatrix(newRows))
    }

  //TODO
  def blackman(window: Int): Seq[Double] = {
    val d = 2 * math.Pi / (window - 1)
    val values = (0 until window) map {
      n => 0.42 - 0.5 * math.cos(n * d) + 0.08 * math.cos(2 * n * d)
    }
    values.map(_ / values.sum)
  }

  //TODO
  def convolve(v1: Seq[Double], v2: Seq[Double]): Seq[Double] = {
    val (n1, n2) = (v1.size, v2.size)
    (0 until n1 + n2 - 1) map { n =>
      val kMin = math.max(0, n - n2 + 1)
      val kMax = math.min(n1 - 1, n)
      (kMin to kMax).map(k => v1(k) * v2(n - k)).sum
    } drop ((n2 - 1) / 2) dropRight ((n2 - 1) / 2)
  }

  //TODO
  def transpose(matrix: IndexedRowMatrix): IndexedRowMatrix =
    matrix.toCoordinateMatrix.transpose.toIndexedRowMatrix

}

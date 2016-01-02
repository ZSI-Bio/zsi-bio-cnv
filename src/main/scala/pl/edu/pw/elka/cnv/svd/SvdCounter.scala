package pl.edu.pw.elka.cnv.svd

import org.apache.commons.math3.linear
import org.apache.commons.math3.linear.{BlockRealMatrix, RealMatrix}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.MetricsContext.rddToInstrumentedRDD
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Main class for calculation of SVD decomposition.
 *
 * @param bedFile Map of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
 * @param zrpkms RDD of (regionId, zrpkms) containing ZRPKM values of given regions.
 * @param svd Number of components to remove.
 * @param reduceWorkers Number of reduce workers to be used (default value - 12).
 */
class SvdCounter(bedFile: Broadcast[mutable.HashMap[Int, (Int, Int, Int)]], zrpkms: RDD[(Int, Array[Double])], svd: Int, reduceWorkers: Int = 12) extends Serializable {

  /**
   * Method for calculation of SVD decomposition based on ZRPKM values given in class constructor.
   *
   * @return RDD of (chr, regions, matrix) containing matrices after SVD decomposition.
   */
  def calculateSvd: RDD[(Int, Array[Int], RealMatrix)] = {
    for {
      (chr, rows) <- prepareRows
      sortedRows = rows.toArray.sortBy(_._1)
      matrix = new BlockRealMatrix(sortedRows.map(_._2))
      svd = new linear.SingularValueDecomposition(matrix)
      newMatrix = reconstructMatrix(svd)
    } yield (chr, sortedRows.map(_._1), newMatrix)
  } instrument()

  /**
   * Method that prepares rows for SVD decomposition.
   *
   * @return RDD of (chr, rows) containing rows for matrices of given chromosomes.
   */
  private def prepareRows: RDD[(Int, ArrayBuffer[(Int, Array[Double])])] = {
    zrpkms.mapPartitions(partition => {
      val rowsMap = new mutable.HashMap[Int, ArrayBuffer[(Int, Array[Double])]]

      for ((regionId, sampleZrpkms) <- partition) {
        val chr = bedFile.value(regionId)._1
        if (!rowsMap.contains(chr))
          rowsMap(chr) = new ArrayBuffer[(Int, Array[Double])]
        rowsMap(chr) += ((regionId, sampleZrpkms))
      }

      rowsMap.iterator
    }).reduceByKey(_ ++ _, reduceWorkers)
  } instrument()

  /**
   * Method that reconstructs matrix after SVD decomposition.
   *
   * @param svd Object containing decomposed matrix.
   * @return Reconstructed matrix.
   */
  private def reconstructMatrix(svd: linear.SingularValueDecomposition): RealMatrix = {
    val newS = removeComponents(svd.getS)
    svd.getU.multiply(newS).multiply(svd.getVT)
  }

  /**
   * Method that removes the greatest singular values.
   *
   * @param matrix Matrix containing singular values.
   * @return Matrix without the greatest singular values.
   */
  private def removeComponents(matrix: RealMatrix): RealMatrix = {
    (0 until svd) foreach {
      x => matrix.setEntry(x, x, 0.0)
    }
    matrix
  }

}

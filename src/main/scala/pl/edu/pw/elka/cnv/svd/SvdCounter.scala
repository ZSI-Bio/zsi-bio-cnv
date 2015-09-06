package pl.edu.pw.elka.cnv.svd

import org.apache.commons.math3.linear
import org.apache.commons.math3.linear.{BlockRealMatrix, RealMatrix}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.utils.ConvertionUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Main class for calculation of SVD decomposition.
 *
 * @param sc Apache Spark context.
 * @param bedFile Array of (regionId, chr, start, end) containing all of the regions to be analyzed.
 * @param zrpkms RDD of (regionId, (sampleId, zrpkm)) containing ZRPKM values for given regions and samples.
 * @param svd Number of components to remove.
 * @param reduceWorkers Number of reduce workers to be used (default value - 12).
 */
class SvdCounter(@transient sc: SparkContext, bedFile: Array[(Int, Int, Int, Int)], zrpkms: RDD[(Int, Iterable[(Int, Double)])],
                 svd: Int, reduceWorkers: Int = 12)
  extends Serializable with ConvertionUtils {

  /**
   * Map of (regionId, chr) containing chromosomes of given regions.
   * It is spread among all of the nodes for quick access.
   */
  private val regionChromosomes: Broadcast[mutable.HashMap[Int, Int]] = sc.broadcast {
    bedFileToRegionChromosomes(bedFile)
  }

  /**
   * Method for calculation of SVD decomposition based on ZRPKM values given in class constructor.
   *
   * @return RDD of (chr, regions, matrix) containing matrices after SVD decomposition.
   */
  def calculateSvd: RDD[(Int, Array[Int], RealMatrix)] =
    for {
      (chr, rows) <- prepareRows
      sortedRows = rows.toArray.sortBy(_._1)
      matrix = new BlockRealMatrix(sortedRows.map(_._2))
      svd = new linear.SingularValueDecomposition(matrix)
      newMatrix = reconstructMatrix(svd)
    } yield (chr, sortedRows.map(_._1), newMatrix)

  /**
   * Method that prepares rows for SVD decomposition.
   *
   * @return RDD of (chr, rows) containing rows for matrices of given chromosomes.
   */
  private def prepareRows: RDD[(Int, ArrayBuffer[(Int, Array[Double])])] =
    zrpkms.mapPartitions(partition => {
      val rowsMap = new mutable.HashMap[Int, ArrayBuffer[(Int, Array[Double])]]

      for ((regionId, sampleZrpkms) <- partition) {
        val chr = regionChromosomes.value(regionId)
        if (!rowsMap.contains(chr))
          rowsMap(chr) = new ArrayBuffer[(Int, Array[Double])]
        rowsMap(chr) += ((regionId, sampleZrpkms.toArray.sorted.map(_._2)))
      }

      rowsMap.iterator
    }).reduceByKey(_ ++ _, reduceWorkers)

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

package pl.edu.pw.elka.cnv.svd

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors}
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.utils.ConvertionUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Main class for calculation of SVD decomposition.
 *
 * @param sc Apache Spark context.
 * @param samples Map of (sampleId, samplePath) containing all of the BAM files.
 * @param bedFile RDD of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
 * @param zrpkms RDD of (regionId, (sampleId, zrpkm)) containing ZRPKM values for given regions and samples.
 * @param svd Number of components to remove.
 * @param reduceWorkers Number of reduce workers to be used (default value - 12).
 */
class SvdCounter(@transient sc: SparkContext, samples: Map[Int, String], bedFile: RDD[(Int, (Int, Int, Int))], zrpkms: RDD[(Int, Iterable[(Int, Double)])],
                 svd: Int, reduceWorkers: Int = 12)
  extends Serializable with ConvertionUtils {

  /**
   * Total number of samples.
   */
  private val samplesCount: Int = samples.size

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
   * @return Array of (chr, matrix) containing matrices of given chromosomes after SVD decomposition.
   */
  def calculateSvd: Array[(Int, IndexedRowMatrix)] =
    for {
      (chr, rows) <- prepareRows.collect
      matrix = new IndexedRowMatrix(sc.makeRDD(rows))
      svd = matrix.computeSVD(samplesCount, true)
      newMatrix = reconstructMatrix(svd)
    } yield (chr, newMatrix)

  /**
   * Method that prepare rows for SVD decomposition.
   *
   * @return RDD of (chr, rows) containing rows of given chromosome's matrix.
   */
  private def prepareRows: RDD[(Int, ArrayBuffer[IndexedRow])] =
    zrpkms.mapPartitions(partition => {
      val rowsMap = new mutable.HashMap[Int, ArrayBuffer[IndexedRow]]

      for ((regionId, sampleZrpkms) <- partition) {
        val chr = regionChromosomes.value(regionId)
        if (!rowsMap.contains(chr))
          rowsMap(chr) = new ArrayBuffer[IndexedRow]
        rowsMap(chr) += new IndexedRow(regionId, Vectors.sparse(samplesCount, sampleZrpkms.toSeq))
      }

      rowsMap.iterator
    }).reduceByKey(_ ++ _, reduceWorkers)

  /**
   * Method that reconstructs matrix after SVD decomposition.
   *
   * @param svd Object containing matrices after decomposition.
   * @return Reconstructed matrix.
   */
  private def reconstructMatrix(svd: SingularValueDecomposition[IndexedRowMatrix, Matrix]): IndexedRowMatrix = {
    val newS = removeComponents(svd.s)
    svd.U.multiply(Matrices.diag(newS)).multiply(svd.V)
  }

  /**
   * Method that removes the greatest singular values.
   *
   * @param vec Vector containing singular values.
   * @return Vector without the greatest singular values.
   */
  private def removeComponents(vec: linalg.Vector): linalg.Vector = {
    val tmp = vec.toArray
    (0 until svd).foreach(tmp.update(_, 0.0))
    Vectors.dense(tmp)
  }

}

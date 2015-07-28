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
 * Created by mariusz-macbook on 25/07/15.
 */
class SvdCounter(@transient sc: SparkContext, samples: Map[Int, String], bedFile: RDD[(Int, (Int, Int, Int))], zrpkms: RDD[(Int, Iterable[(Int, Double)])],
                 k: Int = 5, reduceWorkers: Int = 12)
  extends Serializable with ConvertionUtils {

  private val samplesCount: Int = samples.size

  private val regionsMap: Broadcast[mutable.HashMap[Int, Int]] = sc.broadcast {
    bedFileToRegionsMap(bedFile)
  }

  def calculateSvd: Array[(Int, IndexedRowMatrix)] =
    for {
      (chr, rows) <- prepareRows.collect
      matrix = new IndexedRowMatrix(sc.makeRDD(rows))
      svd = matrix.computeSVD(samplesCount, true)
      newMatrix = reconstructMatrix(svd)
    } yield (chr, newMatrix)

  private def prepareRows: RDD[(Int, ArrayBuffer[IndexedRow])] =
    zrpkms.mapPartitions(partition => {
      val rowsMap = new mutable.HashMap[Int, ArrayBuffer[IndexedRow]]

      for ((regionId, sampleZrpkms) <- partition) {
        val chr = regionsMap.value(regionId)
        if (!rowsMap.contains(chr))
          rowsMap(chr) = new ArrayBuffer[IndexedRow]
        rowsMap(chr) += new IndexedRow(regionId, Vectors.sparse(samplesCount, sampleZrpkms.toSeq))
      }

      rowsMap.iterator
    }).reduceByKey(_ ++ _, reduceWorkers)

  private def reconstructMatrix(svd: SingularValueDecomposition[IndexedRowMatrix, Matrix]): IndexedRowMatrix = {
    val newS = removeComponents(svd.s)
    svd.U.multiply(Matrices.diag(newS)).multiply(svd.V)
  }

  private def removeComponents(vec: linalg.Vector): linalg.Vector = {
    val tmp = vec.toArray
    (0 until k).foreach(tmp.update(_, 0.0))
    Vectors.dense(tmp)
  }

}

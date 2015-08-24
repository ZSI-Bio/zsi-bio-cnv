package pl.edu.pw.elka.cnv.svd

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors}
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.utils.ConvertionUtils

/**
 * Created by mariusz-macbook on 25/07/15.
 */
class SvdCounterTmp(@transient sc: SparkContext, samples: Map[Int, String], zrpkms: RDD[(Int, Iterable[(Int, Double)])], k: Int = 5, reduceWorkers: Int = 12)
  extends Serializable with ConvertionUtils {

  private val samplesCount: Int = samples.size

  def calculateSvd: IndexedRowMatrix = {
    val rows = zrpkms map {
      case (regionId, sampleZrpkms) =>
        new IndexedRow(regionId, Vectors.sparse(samplesCount, sampleZrpkms.toSeq))
    }
    val matrix = new IndexedRowMatrix(rows)
    val svd = matrix.computeSVD(samplesCount, true)

    System.console().printf("Svals " + svd.s.toArray.mkString(" ") + "\n")

    reconstructMatrix(svd)
  }

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
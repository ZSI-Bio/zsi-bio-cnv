package pl.edu.pw.elka.cnv.svd

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.utils.ConvertionUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by mariusz-macbook on 25/07/15.
 */
class SvdCounter(@transient sc: SparkContext, samples: Map[Int, String], bedFile: RDD[(Int, (Int, Int, Int))], zrpkms: RDD[(Int, Iterable[(Int, Double)])])
  extends Serializable with ConvertionUtils {

  private val samplesCount: Int = samples.size

  private val regionsMap: Broadcast[mutable.HashMap[Int, Int]] = sc.broadcast {
    bedFileToRegionsMap(bedFile)
  }

  def calculateSvd =
    zrpkms.mapPartitions(partition => {
      val vectorsMap = new mutable.HashMap[Int, ArrayBuffer[linalg.Vector]]

      for ((regionId, sampleZrpkms) <- partition) {
        val chr = regionsMap.value(regionId)
        if (!vectorsMap.contains(chr))
          vectorsMap(chr) = new ArrayBuffer[linalg.Vector]
        vectorsMap(chr) += Vectors.dense(sampleZrpkms.toArray.map(_._2))
      }

      vectorsMap.iterator
    }).reduceByKeyLocally(_ ++ _) map {
      case (chr, rows) =>
        new RowMatrix(sc.makeRDD(rows)).computeSVD(samplesCount)
    } map (_.s.toArray.mkString("\t")) foreach (println)

}

package pl.edu.pw.elka.cnv.caller

import org.apache.commons.math3.linear.RealMatrix
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.utils.ConvertionUtils

import scala.collection.mutable

/**
 * Created by mariusz-macbook on 30/08/15.
 */
class Caller(sc: SparkContext, bedFile: Array[(Int, Int, Int, Int)], matrices: RDD[(Int, Array[Int], RealMatrix)], threshold: Double)
  extends Serializable with ConvertionUtils {

  private val regionCoords: Broadcast[mutable.HashMap[Int, (Int, Int)]] = sc.broadcast {
    bedFileToRegionCoords(bedFile)
  }

  def call: RDD[(Int, Int, Int, Int, String)] =
    for {
      (chr, regions, matrix) <- matrices
      caller = new ChrCaller(regions, matrix, threshold)
      (sampleId, start, stop, state) <- caller.call
    } yield (sampleId, chr, getStart(start), getStop(stop), state)

  private def getStart(regId: Int): Int =
    regionCoords.value(regId)._1

  private def getStop(regId: Int): Int =
    regionCoords.value(regId)._2

}

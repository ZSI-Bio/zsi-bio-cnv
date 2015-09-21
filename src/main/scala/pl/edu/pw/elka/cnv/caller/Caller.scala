package pl.edu.pw.elka.cnv.caller

import org.apache.commons.math3.linear.RealMatrix
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Created by mariusz-macbook on 30/08/15.
 */
class Caller(bedFile: Broadcast[mutable.HashMap[Int, (Int, Int, Int)]], matrices: RDD[(Int, Array[Int], RealMatrix)], threshold: Double) extends Serializable {

  def call: RDD[(Int, Int, Int, Int, String)] =
    for {
      (chr, regions, matrix) <- matrices
      caller = new ChrCaller(regions, matrix, threshold)
      (sampleId, start, stop, state) <- caller.call
    } yield (sampleId, chr, getStart(start), getStop(stop), state)

  private def getStart(regId: Int): Int =
    bedFile.value(regId)._2

  private def getStop(regId: Int): Int =
    bedFile.value(regId)._3

}

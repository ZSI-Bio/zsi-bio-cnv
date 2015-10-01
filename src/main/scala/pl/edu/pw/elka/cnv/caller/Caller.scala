package pl.edu.pw.elka.cnv.caller

import org.apache.commons.math3.linear.RealMatrix
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Main class for making calls.
 *
 * @param bedFile Map of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
 * @param matrices RDD of (chr, regions, matrix) containing matrices after SVD decomposition.
 * @param threshold +/- Threshold for calling (minimum SVD-ZRPKM).
 */
class Caller(bedFile: Broadcast[mutable.HashMap[Int, (Int, Int, Int)]], matrices: RDD[(Int, Array[Int], RealMatrix)], threshold: Double)
  extends Serializable {

  /**
   * Main method for making calls.
   * It invokes detection of CNV mutations in each chromosome and then
   * resolves starts and stops of detected mutations based on region identifiers.
   *
   * @return RDD of (sampleId, chr, start, stop, state) containing detected CNV mutations.
   */
  def call: RDD[(Int, Int, Int, Int, String)] =
    for {
      (chr, regions, matrix) <- matrices
      caller = new ChrCaller(regions, matrix, threshold)
      (sampleId, startId, stopId, state) <- caller.call
    } yield (sampleId, chr, getStart(startId), getStop(stopId), state)

  /**
   * Method that resolves start of given region.
   *
   * @param regId Region identifier.
   * @return Start of given region.
   */
  private def getStart(regId: Int): Int =
    bedFile.value(regId)._2

  /**
   * Method that resolves stop of given region.
   *
   * @param regId Region identifier.
   * @return Stop of given region.
   */
  private def getStop(regId: Int): Int =
    bedFile.value(regId)._3

}

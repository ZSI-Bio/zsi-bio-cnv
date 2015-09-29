package pl.edu.pw.elka.cnv.rpkm

import htsjdk.samtools.SAMRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.utils.CNVUtils

import scala.collection.mutable

/**
 * Main class for calculation of RPKM values.
 *
 * @param reads RDD of (sampleId, read) containing all of the reads to be analyzed.
 * @param bedFile Map of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
 * @param coverage RDD of (regionId, (sampleId, coverage)) containing coverage of given regions by given samples.
 */
class RpkmsCounter(reads: RDD[(Int, SAMRecord)], bedFile: Broadcast[mutable.HashMap[Int, (Int, Int, Int)]], coverage: RDD[(Int, Iterable[(Int, Int)])])
  extends Serializable with CNVUtils {

  /**
   * Map of (sampleId, total) containing total number of reads in given samples.
   * It is spread among all of the nodes for quick access.
   */
  private val readCounts: collection.Map[Int, Long] = reads.countByKey

  /**
   * Method for calculation of RPKM values based on coverage given in class constructor.
   *
   * @return RDD of (regionId, rpkms) containing calculated RPKM values.
   */
  def calculateRpkms: RDD[(Int, Array[Double])] =
    coverage map {
      case (regionId, sampleCoverages) =>
        val (_, start, stop) = bedFile.value(regionId)
        val sampleRpkms = fillWithZeros(sampleCoverages) map {
          case (coverage, sampleId) => rpkm(coverage, stop - start, readCounts(sampleId))
        }
        (regionId, sampleRpkms)
    }

  /**
   * Method that puts zeros in place of no coverage value.
   *
   * @param sampleCoverages Iterable of (sampleId, coverage) containing coverage of given samples.
   * @return Array of (coverage, sampleId) containing coverage of all samples.
   */
  private def fillWithZeros(sampleCoverages: Iterable[(Int, Int)]): Array[(Int, Int)] = {
    val result = new Array[Int](readCounts.size)
    sampleCoverages.foreach(x => result(x._1) = x._2)
    result.zipWithIndex
  }

}

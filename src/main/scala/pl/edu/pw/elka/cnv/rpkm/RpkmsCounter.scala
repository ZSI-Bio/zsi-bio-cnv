package pl.edu.pw.elka.cnv.rpkm

import htsjdk.samtools.SAMRecord
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.utils.{ConvertionUtils, StatUtils}

import scala.collection.mutable

/**
 * Main class for calculation of RPKM values.
 *
 * @param reads RDD of (sampleId, read) containing all of the reads to be analyzed.
 * @param bedFile RDD of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
 * @param coverage RDD of (regionId, (sampleId, coverage)) containing coverage of given regions by given samples.
 */
class RpkmsCounter(@transient sc: SparkContext, reads: RDD[(Int, SAMRecord)], bedFile: RDD[(Int, (Int, Int, Int))], coverage: RDD[(Int, Iterable[(Int, Int)])])
  extends Serializable with ConvertionUtils with StatUtils {

  /**
   * Map of (sampleId, total) containing total number of reads in given samples.
   */
  private val readCounts: Broadcast[collection.Map[Int, Long]] = sc.broadcast {
    reads.countByKey
  }

  private val regionLengths: Broadcast[mutable.HashMap[Int, Int]] = sc.broadcast {
    bedFileToRegionLengths(bedFile)
  }

  /**
   * Method for calculation of RPKM values based on reads, BED file and coverage given in class constructor.
   *
   * @return RDD of (regionId, (sampleId, rpkm)) containing calculated RPKM values.
   */
  def calculateRpkms: RDD[(Int, Iterable[(Int, Double)])] =
    coverage flatMap {
      case (regionId, sampleCoverages) => sampleCoverages map {
        case (sampleId, coverage) =>
          (regionId, (sampleId, rpkm(coverage, regionLengths.value(regionId), readCounts.value(sampleId))))
      }
    } groupByKey

}

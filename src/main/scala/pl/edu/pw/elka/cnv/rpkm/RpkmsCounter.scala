package pl.edu.pw.elka.cnv.rpkm

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.utils.StatUtils

/**
 * Main class for calculation of RPKM values.
 *
 * @param reads RDD of (sampleId, read) containing all of the reads to be analyzed.
 * @param bedFile RDD of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
 * @param coverage RDD of (regionId, (sampleId, coverage)) containing coverage of given regions by given samples.
 */
class RpkmsCounter(reads: RDD[(Int, SAMRecord)], bedFile: RDD[(Int, (Int, Int, Int))], coverage: RDD[(Int, Iterable[(Int, Int)])])
  extends Serializable with StatUtils {

  /**
   * Map of (sampleId, total) containing total number of reads in given samples.
   */
  private val readsCount: collection.Map[Int, Long] = reads.countByKey

  /**
   * Method for calculation of RPKM values based on reads, BED file and coverage given in class constructor.
   *
   * @return RDD of (regionId, (sampleId, rpkm)) containing calculated RPKM values.
   */
  def calculateRpkms: RDD[(Int, Iterable[(Int, Double)])] =
    bedFile join coverage flatMap {
      case (regionId, ((_, start, end), samplesCoverage)) => samplesCoverage map {
        case (sampleId, coverage) => (regionId, (sampleId, rpkm(coverage, end - start, readsCount(sampleId))))
      }
    } groupByKey

}

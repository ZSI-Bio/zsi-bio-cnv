package pl.edu.pw.elka.cnv.utils

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Object containing methods for various data conversions.
 */
object ConversionUtils {

  /**
   * Method for converting data from BED file into map optimized for searching by chromosome and position.
   *
   * @param bedFile Map of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
   * @return Map of (chr, (regionId, start end)) optimized for calculating coverage.
   */
  def bedFileToChromosomesMap(bedFile: mutable.HashMap[Int, (Int, Int, Int)]): mutable.HashMap[Int, Array[ArrayBuffer[(Int, Int, Int)]]] = {
    val result = new mutable.HashMap[Int, Array[ArrayBuffer[(Int, Int, Int)]]]
    for ((regionId, (chr, start, end)) <- bedFile) {
      if (!result.contains(chr))
        result(chr) = new Array[ArrayBuffer[(Int, Int, Int)]](25000)
      val startId = start / 10000
      if (result(chr)(startId) == null)
        result(chr)(startId) = new ArrayBuffer[(Int, Int, Int)]
      result(chr)(startId) += ((regionId, start, end))
    }
    result
  }

  /**
   * Method for converting data from BED file into map of region's chromosomes.
   *
   * @param bedFile Array of (regionId, chr, start, end) containing all of the regions to be analyzed.
   * @return Map of (regionId, chr) containing chromosomes of given regions.
   */
  def bedFileToRegionChromosomes(bedFile: Array[(Int, Int, Int, Int)]): mutable.HashMap[Int, Int] = {
    val result = new mutable.HashMap[Int, Int]
    bedFile foreach {
      case (regionId, chr, _, _) =>
        result(regionId) = chr
    }
    result
  }

  /**
   * Method for converting data from BED file into map of region's lengths.
   *
   * @param bedFile Array of (regionId, chr, start, end) containing all of the regions to be analyzed.
   * @return Map of (regionId, length) containing lengths of given regions.
   */
  def bedFileToRegionLengths(bedFile: Array[(Int, Int, Int, Int)]): mutable.HashMap[Int, Int] = {
    val result = new mutable.HashMap[Int, Int]
    bedFile foreach {
      case (regionId, _, start, end) =>
        result(regionId) = (end - start + 1)
    }
    result
  }

  /**
   * Method for converting data from BED file into map of region's coords.
   *
   * @param bedFile Array of (regionId, chr, start, end) containing all of the regions to be analyzed.
   * @return Map of (regionId, length) containing coords of given regions.
   */
  def bedFileToRegionCoords(bedFile: Array[(Int, Int, Int, Int)]): mutable.HashMap[Int, (Int, Int)] = {
    val result = new mutable.HashMap[Int, (Int, Int)]
    bedFile foreach {
      case (regionId, _, start, end) =>
        result(regionId) = (start, end)
    }
    result
  }

  /**
   * Method for converting internal coverage representation used for efficiency purposes into RDD optimized for searching by region ID.
   *
   * @param coverage RDD of (coverageId, coverage) containing coverage in internal representation.
   * @return RDD of (regionId, (sampleId, coverage)) containing coverage optimized for searching by region ID.
   */
  def coverageToRegionCoverage(coverage: RDD[(Long, Int)]): RDD[(Int, Iterable[(Int, Int)])] =
    coverage map {
      case (coverageId, coverage) =>
        val (sampleId, regionId) = decodeCoverageId(coverageId)
        (regionId, (sampleId, coverage))
    } groupByKey

  /**
   * Method for converting internal coverage representation used for efficiency purposes into RDD optimized for searching by region ID.
   *
   * @param coverage RDD of (coverageId, coverage) containing coverage in internal representation.
   * @return RDD of (regionId, (sampleId, coverage)) containing mean coverage optimized for searching by region ID.
   */
  def coverageToMeanRegionCoverage(coverage: RDD[(Long, Int)], bedFile: Broadcast[mutable.HashMap[Int, (Int, Int, Int)]]): RDD[(Int, Iterable[(Int, Double)])] =
    coverage map {
      case (coverageId, coverage) =>
        val (sampleId, regionId) = decodeCoverageId(coverageId)
        val (_, start, end) = bedFile.value(regionId)
        val meanCoverage = coverage.toDouble / (end - start + 1)
        (regionId, (sampleId, meanCoverage))
    } groupByKey

  /**
   * Method for converting sampleId and regionId into single coverageId used for efficiency purposes.
   *
   * @param sampleId Id of a given sample.
   * @param regionId Id of a given region.
   * @return Id consisting of sampleId and regionId.
   */
  def encodeCoverageId(sampleId: Int, regionId: Int): Long =
    sampleId * 1000000000L + regionId

  /**
   * Method for converting single coverageId used for efficiency purposes into sampleId and regionId.
   *
   * @param coverageId Id consisting of sampleId and regionId.
   * @return Tuple (sampleId, regionId).
   */
  def decodeCoverageId(coverageId: Long): (Int, Int) =
    ((coverageId / 1000000000L).toInt, (coverageId % 1000000000L).toInt)

  /**
   * Method for converting chromosome names from strings to integers.
   *
   * @param chr Chromosome name as string.
   * @return Chromosome name as integer.
   */
  def chrStrToInt(chr: String): Int =
    chr.replace("chr", "") match {
      case "X" => 23
      case "Y" => 24
      case tmp =>
        if (tmp matches "\\d*") tmp.toInt
        else 0
    }

}

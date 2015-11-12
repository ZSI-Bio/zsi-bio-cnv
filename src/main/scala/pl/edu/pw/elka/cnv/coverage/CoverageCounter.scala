package pl.edu.pw.elka.cnv.coverage

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.filter.ReadFilter
import pl.edu.pw.elka.cnv.model.CNVRecord
import pl.edu.pw.elka.cnv.utils.ConvertionUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Main class for calculation of coverage.
 *
 * @param sc Apache Spark context.
 * @param bedFile Map of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
 * @param reads RDD of (sampleId, read) containing all of the reads to be analyzed.
 * @param parseCigar Flag indicating whether or not to parse a cigar string (default value - false).
 * @param countingMode Mode of coverage calculation to be used (default value - CountingMode.COUNT_WHEN_STARTS).
 * @param reduceWorkers Number of reduce workers to be used (default value - 12).
 */
class CoverageCounter(@transient sc: SparkContext, bedFile: Broadcast[mutable.HashMap[Int, (Int, Int, Int)]], reads: RDD[(Int, CNVRecord)],
                      readFilters: Array[ReadFilter] = Array(), parseCigar: Boolean = false, countingMode: Int = CountingMode.COUNT_WHEN_STARTS, reduceWorkers: Int = 12)
  extends Serializable with ConvertionUtils {

  /**
   * Map of (chr, (regionId, start, end)) optimized for searching by chromosome and position.
   * It is spread among all of the nodes for quick access.
   */
  private val chromosomesMap: Broadcast[mutable.HashMap[Int, Array[ArrayBuffer[(Int, Int, Int)]]]] = sc.broadcast {
    bedFileToChromosomesMap(bedFile.value)
  }

  private val filteredReads: RDD[(Int, CNVRecord)] =
    if (readFilters.isEmpty) reads
    else reads filter {
      case (_, read) => readFilters forall {
        filter => !filter.filterOut(read)
      }
    }

  /**
   * Method for calculation of coverage based on regions and reads given in class constructor.
   * It returns coverage in an internal representation for efficiency purposes. One can convert it using [[coverageToRegionCoverage]] method.
   *
   * @return RDD of (coverageId, coverage). For more information about coverageId see [[encodeCoverageId]] method.
   */
  def calculateReadCoverage: RDD[(Long, Int)] =
    filteredReads.mapPartitions(partition => {
      val regionsCountMap = new mutable.HashMap[Long, Int]

      for ((sampleId, read) <- partition)
        if (chromosomesMap.value.contains(read.getReferenceName)) {
          val regions = chromosomesMap.value(read.getReferenceName)
          val blocks = generateBlocks(read)
          for ((blockStart, blockEnd) <- blocks) {
            val regionsToCheck = getRegionsToCheck(blockStart, regions)
            if (regionsToCheck != null)
              for ((regionId, regionStart, regionEnd) <- regionsToCheck)
                if (countingCondition(blockStart, blockEnd, regionStart, regionEnd)) {
                  val coverageId = encodeCoverageId(sampleId, regionId)
                  if (!regionsCountMap.contains(coverageId))
                    regionsCountMap(coverageId) = 1
                  else
                    regionsCountMap(coverageId) += 1
                }
          }
        }

      regionsCountMap.iterator
    }).reduceByKey(_ + _, reduceWorkers)

  def calculateBaseCoverage: RDD[(Long, Int)] =
    filteredReads.mapPartitions(partition => {
      val regionsCountMap = new mutable.HashMap[Long, Int]

      for ((sampleId, read) <- partition)
        if (chromosomesMap.value.contains(read.getReferenceName)) {
          val regions = chromosomesMap.value(read.getReferenceName)
          val blocks = generateBlocks(read)
          for ((blockStart, blockEnd) <- blocks) {
            val regionsToCheck = getRegionsToCheck(blockStart, regions)
            if (regionsToCheck != null)
              for ((regionId, regionStart, regionEnd) <- regionsToCheck)
                if (countingCondition(blockStart, blockEnd, regionStart, regionEnd)) {
                  val coverageId = encodeCoverageId(sampleId, regionId)
                  val overlappingBases = math.min(blockEnd, regionEnd) - math.max(blockStart, regionStart) + 1
                  if (!regionsCountMap.contains(coverageId))
                    regionsCountMap(coverageId) = overlappingBases
                  else
                    regionsCountMap(coverageId) += overlappingBases
                }
          }
        }

      regionsCountMap.iterator
    }).reduceByKey(_ + _, reduceWorkers)

  /**
   * Method returning flag that determines whether or not given base covers given region according to a chosen counting mode.
   *
   * @param baseStart Starting position of a given base.
   * @param baseEnd Ending position of a given base.
   * @param regionStart Starting position of a given region.
   * @param regionEnd Starting position of a given region.
   * @return Boolean flag.
   */
  private def countingCondition(blockStart: Int, blockEnd: Int, regionStart: Int, regionEnd: Int): Boolean =
    countingMode match {
      case CountingMode.COUNT_WHEN_WHITIN =>
        if (blockStart >= regionStart && blockEnd <= regionEnd) true
        else false
      case CountingMode.COUNT_WHEN_OVERLAPS =>
        if ((blockStart >= regionStart && blockStart <= regionEnd)
          || (blockStart <= regionStart && blockEnd >= regionEnd)
          || (blockEnd >= regionStart && blockEnd <= regionEnd)) true
        else false
      case CountingMode.COUNT_WHEN_STARTS =>
        if (blockStart >= regionStart && blockStart <= regionEnd) true
        else false
    }

  /**
   * Method returning regions that a given base may overlap. It optimizes calculation of coverage by narrowing area of interest to nearby regions.
   *
   * @param baseStart Starting position of a given base.
   * @param regions Array of (regionId, start end) optimized for searching by position.
   * @return Array of (regionId, start end) containing nearby regions.
   */
  private def getRegionsToCheck(baseStart: Int, regions: Array[ArrayBuffer[(Int, Int, Int)]]): ArrayBuffer[(Int, Int, Int)] = {
    val startId = baseStart / 10000
    var result = regions(startId)

    if (startId > 0 && regions(startId - 1) != null)
      if (result != null) result = result ++ regions(startId - 1)
      else result = regions(startId - 1)

    result
  }

  /**
   * Method generating bases from given read by parsing a cigar string. If parseCigar is set to false it simply returns (readStart, readEnd).
   *
   * @param read Read to be analyzed.
   * @return Array of (baseStart, baseEnd) containing all of the bases generated from a given read.
   */
  private def generateBlocks(read: CNVRecord): Array[(Int, Int)] =
    if (parseCigar) genBasesFromCigar(read)
    else Array((read.getAlignmentStart, read.getAlignmentEnd))

  /**
   * Method generating bases by parsing a cigar string.
   *
   * @param alignStart Alignment start position of a read.
   * @param cigar Cigar string of a read.
   * @return Array of (baseStart, baseEnd) containing all of the bases generated from a given cigar string.
   */
  private def genBasesFromCigar(read: CNVRecord): Array[(Int, Int)] =
    read.getAlignmentBlocks map {
      block => (block.getReferenceStart, block.getReferenceStart + block.getLength - 1)
    }

}

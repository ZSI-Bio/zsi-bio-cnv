package pl.edu.pw.elka.cnv.coverage

import htsjdk.samtools.SAMRecord
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.utils.ConvertionUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Main class for calculation of coverage.
 *
 * @param sc Apache Spark context.
 * @param bedFile RDD of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
 * @param reads RDD of (sampleId, read) containing all of the reads to be analyzed.
 * @param parseCigar Flag indicating whether or not to parse a cigar string (default value - false).
 * @param countingMode Mode of coverage calculation to be used (default value - CountingMode.COUNT_WHEN_STARTS).
 * @param reduceWorkers Number of reduce workers to be used (default value - 12).
 */
class CoverageCounter(@transient sc: SparkContext, bedFile: RDD[(Int, (String, Int, Int))], reads: RDD[(Int, SAMRecord)],
                      parseCigar: Boolean = false, countingMode: Int = CountingMode.COUNT_WHEN_STARTS, reduceWorkers: Int = 12)
  extends Serializable with ConvertionUtils {

  /**
   * Map of (chr, (regionId, start end)) optimized for searching by chromosome and position.
   * It is spread among all of the nodes for quick access.
   */
  private val regionsMap: Broadcast[mutable.HashMap[String, Array[ArrayBuffer[(Int, Int, Int)]]]] = sc.broadcast {
    bedFileToRegionsMap(bedFile)
  }

  /**
   * Method for calculation of coverage based on regions and reads given in class constructor.
   * It returns coverage in an internal representation for efficiency purposes. One can convert it using methods from ConvertionUtils.
   *
   * @return RDD of (coverageId, coverage). For more information about coverageId see encodeCoverageId method.
   */
  def calculateCoverage: RDD[(Long, Int)] =
    reads.mapPartitions(partition => {
      val regionsCountMap = new mutable.HashMap[Long, Int]

      for ((sampleId, read) <- partition)
        if (regionsMap.value.contains(read.getReferenceName)) {
          val regions = regionsMap.value(read.getReferenceName)
          val bases = genBases(read)
          for ((baseStart, baseEnd) <- bases) {
            val regionsToCheck = getRegionsToCheck(baseStart, regions)
            if (regionsToCheck != null)
              for ((regionId, regionStart, regionEnd) <- regionsToCheck)
                if (countingCondition(baseStart, baseEnd, regionStart, regionEnd)) {
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

  /**
   * Method returning flag that determines whether or not given base covers given region according to a chosen counting mode.
   *
   * @param baseStart Starting position of a given base.
   * @param baseEnd Ending position of a given base.
   * @param regionStart Starting position of a given region.
   * @param regionEnd Starting position of a given region.
   * @return Boolean flag.
   */
  private def countingCondition(baseStart: Int, baseEnd: Int, regionStart: Int, regionEnd: Int): Boolean =
    countingMode match {
      case CountingMode.COUNT_WHEN_WHITIN =>
        if (baseStart >= regionStart && baseEnd <= regionEnd) true
        else false
      case CountingMode.COUNT_WHEN_OVERLAPS =>
        if ((baseStart >= regionStart && baseStart <= regionEnd)
          || (baseStart <= regionStart && baseEnd >= regionEnd)
          || (baseEnd >= regionStart && baseEnd <= regionEnd)) true
        else false
      case CountingMode.COUNT_WHEN_STARTS =>
        if (baseStart >= regionStart && baseStart <= regionEnd) true
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

    if (startId > 0 && result != null && regions(startId - 1) != null)
      result = result ++ (regions(startId - 1))
    else if (startId > 0 && result == null && regions(startId - 1) != null)
      result = regions(startId - 1)

    result
  }

  /**
   * Method generating bases from given read by parsing a cigar string. If parseCigar is set to false it simply returns (readStart, readEnd).
   *
   * @param read Read to be analyzed.
   * @return Array of (baseStart, baseEnd) containing all of the bases generated from a given read.
   */
  private def genBases(read: SAMRecord): ArrayBuffer[(Int, Int)] =
    if (parseCigar) genBasesFromCigar(read.getAlignmentStart, read.getCigar)
    else ArrayBuffer((read.getAlignmentStart, read.getAlignmentEnd))

  /**
   * Method generating bases by parsing a cigar string.
   *
   * @param alignStart Alignment start position of a read.
   * @param cigar Cigar string of a read.
   * @return Array of (baseStart, baseEnd) containing all of the bases generated from a given cigar string.
   */
  private def genBasesFromCigar(alignStart: Int, cigar: htsjdk.samtools.Cigar): ArrayBuffer[(Int, Int)] = {
    val result = new ArrayBuffer[(Int, Int)]
    val numCigElem = cigar.numCigarElements

    var shift = 0
    for (i <- 0 until numCigElem) {
      val cigElem = cigar.getCigarElement(i)
      if ((i == 0 && cigElem.getOperator.toString == "M") || (i == 1 && cigar.getCigarElement(0).getOperator.toString == "S"))
        result += ((alignStart, alignStart + cigElem.getLength))
      else if (cigElem.getOperator.toString != "M")
        shift += cigElem.getLength
      else if (i > 1 && result.length > 0 && cigElem.getOperator.toString == "M") {
        val temp = result.last._2 + shift
        result += ((temp, temp + cigElem.getLength))
        shift = 0
      }
    }

    result
  }

}

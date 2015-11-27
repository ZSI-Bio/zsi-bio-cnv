package pl.edu.pw.elka.cnv.application

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.MetricsContext.rddToInstrumentedRDD
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.coverage.{CountingMode, CoverageCounter}
import pl.edu.pw.elka.cnv.filter._
import pl.edu.pw.elka.cnv.model.CNVRecord
import pl.edu.pw.elka.cnv.utils.FileUtils

import scala.collection.mutable

/**
 * Main class for calculation of DepthOfCoverage.
 *
 * @param sc Apache Spark context.
 * @param bedFilePath Path to folder containing BED file.
 * @param bamFilesPath Path to folder containing BAM files.
 */
class DepthOfCoverage(@transient sc: SparkContext, bedFilePath: String, bamFilesPath: String) extends Serializable with FileUtils {

  /**
   * Map of (sampleId, samplePath) containing all of the found BAM files.
   */
  private val samples: Map[Int, String] = scanForSamples(bamFilesPath)

  /**
   * RDD of (sampleId, read) containing all of the reads to be analyzed.
   */
  private val reads: RDD[(Int, CNVRecord)] = loadReads(sc, samples).instrument()

  /**
   * Map of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
   */
  private val bedFile: Broadcast[mutable.HashMap[Int, (Int, Int, Int)]] = sc.broadcast {
    readIntervalFile(bedFilePath)
  }

  /**
   * Method that calculates DepthOfCoverage based on files given as input.
   *
   * @return RDD of (regionId, (sampleId, coverage)) containing calculated coverage.
   */
  def calculate: RDD[(Int, Iterable[(Int, Double)])] = {
    val filters = Array(
      new NotPrimaryAlignmentFilter,
      new FailsVendorQualityCheckFilter,
      new DuplicateReadFilter,
      new UnmappedReadFilter,
      new MalformedReadFilter,
      new BadCigarFilter,
      new BadMappingQualityFilter(20, Int.MaxValue)
    )

    val counter = new CoverageCounter(sc, bedFile, reads, filters, true, CountingMode.COUNT_WHEN_OVERLAPS)
    coverageToMeanRegionCoverage(counter.calculateBaseCoverage, bedFile)
  }

}
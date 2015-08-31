package pl.edu.pw.elka.cnv.conifer

import htsjdk.samtools.SAMRecord
import org.apache.commons.math3.linear.RealMatrix
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.caller.Caller
import pl.edu.pw.elka.cnv.coverage.CoverageCounter
import pl.edu.pw.elka.cnv.rpkm.RpkmsCounter
import pl.edu.pw.elka.cnv.svd.SvdCounter
import pl.edu.pw.elka.cnv.utils.{CNVUtils, ConvertionUtils, FileUtils, StatUtils}
import pl.edu.pw.elka.cnv.zrpkm.ZrpkmsCounter

/**
 * Main class for CoNIFER algorithm.
 *
 * @param sc Apache Spark context.
 * @param bedFilePath Path to folder containing BED file.
 * @param bamFilesPath Path to folder containing BAM files.
 * @param minMedian Minimum population median RPKM per probe (default value - 1.0).
 * @param svd Number of components to remove (default value - 12).
 * @param threshold +/- threshold for calling (minimum SVD-ZRPKM) (default value - 1.5).
 */
class Conifer(@transient sc: SparkContext, bedFilePath: String, bamFilesPath: String, minMedian: Double = 1.0, svd: Int = 12, threshold: Double = 1.5)
  extends Serializable with ConvertionUtils with CNVUtils with FileUtils with StatUtils {

  /**
   * Map of (sampleId, samplePath) containing all of the found BAM files.
   */
  private val samples: Map[Int, String] = scanForSamples(bamFilesPath)

  /**
   * RDD of (sampleId, read) containing all of the reads to be analyzed.
   */
  private val reads: RDD[(Int, SAMRecord)] = loadReads(sc, samples)

  /**
   * RDD of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
   */
  private val bedFile: RDD[(Int, (Int, Int, Int))] = readBedFile(sc, bedFilePath)

  /**
   * Method for calculation of coverage.
   *
   * @return RDD of (regionId, (sampleId, coverage)) containing calculated coverage.
   */
  def coverage: RDD[(Int, Iterable[(Int, Int)])] = {
    val counter = new CoverageCounter(sc, bedFile, reads)
    coverageToRegionCoverage(counter.calculateCoverage)
  }

  /**
   * Method for calculation of RPKM values.
   *
   * @param coverage RDD of (regionId, (sampleId, coverage)) containing coverage.
   * @return RDD of (regionId, (sampleId, rpkm)) containing calculated RPKM values.
   */
  def rpkms(coverage: RDD[(Int, Iterable[(Int, Int)])]): RDD[(Int, Iterable[(Int, Double)])] = {
    val counter = new RpkmsCounter(sc, reads, bedFile, coverage)
    counter.calculateRpkms
  }

  /**
   * Method for calculation of ZRPKM values.
   *
   * @param rpkms RDD of (regionId, (sampleId, rpkm)) containing RPKM values.
   * @return RDD of (regionId, (sampleId, zrpkm)) containing calculated ZRPKM values.
   */
  def zrpkms(rpkms: RDD[(Int, Iterable[(Int, Double)])]): RDD[(Int, Iterable[(Int, Double)])] = {
    val counter = new ZrpkmsCounter(samples, rpkms, minMedian)
    counter.calculateZrpkms
  }

  /**
   * Method for calculation of SVD decomposition.
   *
   * @param zrpkms RDD of (regionId, (sampleId, zrpkm)) containing ZRPKM values.
   * @return RDD of (chr, matrix) containing matrices after SVD decomposition.
   */
  def svd(zrpkms: RDD[(Int, Iterable[(Int, Double)])]): RDD[(Int, RealMatrix)] = {
    val counter = new SvdCounter(sc, bedFile, zrpkms, svd)
    counter.calculateSvd
  }

  /**
   * Method for making calls.
   *
   * @param matrices RDD of (chr, matrix) containing matrices after SVD decomposition.
   * @return RDD of (chr, (sampleId, start, stop, state) containing detected CNV mutations.
   */
  def call(matrices: RDD[(Int, RealMatrix)]): RDD[(Int, Array[(Int, Int, Int, String)])] =
    for {
      (chr, matrix) <- matrices
      caller = new Caller(matrix, threshold)
    } yield (chr, caller.call)

}

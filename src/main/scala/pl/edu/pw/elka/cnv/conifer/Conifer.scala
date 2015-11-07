package pl.edu.pw.elka.cnv.conifer

import org.apache.commons.math3.linear.RealMatrix
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.caller.Caller
import pl.edu.pw.elka.cnv.coverage.CoverageCounter
import pl.edu.pw.elka.cnv.model.CNVRecord
import pl.edu.pw.elka.cnv.rpkm.RpkmsCounter
import pl.edu.pw.elka.cnv.svd.SvdCounter
import pl.edu.pw.elka.cnv.utils.FileUtils
import pl.edu.pw.elka.cnv.zrpkm.ZrpkmsCounter

import scala.collection.mutable

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
  extends Serializable with FileUtils {

  /**
   * Map of (sampleId, samplePath) containing all of the found BAM files.
   */
  private val samples: Map[Int, String] = scanForSamples(bamFilesPath)

  /**
   * RDD of (sampleId, read) containing all of the reads to be analyzed.
   */
  private val reads: RDD[(Int, CNVRecord)] = loadReads(sc, samples)

  /**
   * Map of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
   */
  private val bedFile: Broadcast[mutable.HashMap[Int, (Int, Int, Int)]] = sc.broadcast {
    readBedFile(bedFilePath)
  }

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
   * @return RDD of (regionId, rpkms) containing calculated RPKM values.
   */
  def rpkms(coverage: RDD[(Int, Iterable[(Int, Int)])]): RDD[(Int, Array[Double])] = {
    val counter = new RpkmsCounter(reads, bedFile, coverage)
    counter.calculateRpkms
  }

  /**
   * Method for calculation of ZRPKM values.
   *
   * @param rpkms RDD of (regionId, rpkms) containing RPKM values.
   * @return RDD of (regionId, zrpkms) containing calculated ZRPKM values.
   */
  def zrpkms(rpkms: RDD[(Int, Array[Double])]): RDD[(Int, Array[Double])] = {
    val counter = new ZrpkmsCounter(rpkms, minMedian)
    counter.calculateZrpkms
  }

  /**
   * Method for calculation of SVD decomposition.
   *
   * @param zrpkms RDD of (regionId, zrpkms) containing ZRPKM values.
   * @return RDD of (chr, regions, matrix) containing matrices after SVD decomposition.
   */
  def svd(zrpkms: RDD[(Int, Array[Double])]): RDD[(Int, Array[Int], RealMatrix)] = {
    val counter = new SvdCounter(bedFile, zrpkms, svd)
    counter.calculateSvd
  }

  /**
   * Method for making calls.
   *
   * @param matrices RDD of (chr, regions, matrix) containing matrices after SVD decomposition.
   * @return RDD of (sampleId, chr, start, stop, state) containing detected CNV mutations.
   */
  def call(matrices: RDD[(Int, Array[Int], RealMatrix)]): RDD[(Int, Int, Int, Int, String)] = {
    val caller = new Caller(bedFile, matrices, threshold)
    caller.call
  }

  /**
   * Method that performs all steps of CoNIFER algorithm.
   *
   * @return RDD of (sampleId, chr, start, stop, state) containing detected CNV mutations.
   */
  def calculate: RDD[(Int, Int, Int, Int, String)] = {

    // 1. Calculate coverage
    val calculatedCoverage = coverage

    // 2. Calculare RPKM values
    val calculatedRpkms = rpkms(calculatedCoverage)

    // 3. Calculate ZRPKM values
    val calculatedZrpkms = zrpkms(calculatedRpkms)

    // 4. Calculate SVD-ZRPKM values
    val calculatedMatrices = svd(calculatedZrpkms)

    // 5. Make calls
    val calculatedCalls = call(calculatedMatrices)

    // 6. Return detected CNV mutations
    calculatedCalls
  }

}

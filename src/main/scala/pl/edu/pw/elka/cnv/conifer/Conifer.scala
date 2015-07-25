package pl.edu.pw.elka.cnv.conifer

import htsjdk.samtools.SAMRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.coverage.CoverageCounter
import pl.edu.pw.elka.cnv.rpkm.RpkmsCounter
import pl.edu.pw.elka.cnv.svd.SvdCounter
import pl.edu.pw.elka.cnv.utils.{ConvertionUtils, FileUtils}
import pl.edu.pw.elka.cnv.zrpkm.ZrpkmsCounter

/**
 * Main class for CoNIFER algorithm.
 *
 * @param sc Apache Spark context.
 * @param bedFilePath Path to folder containing BED file.
 * @param bamFilesPath Path to folder containing BAM files.
 */
class Conifer(@transient sc: SparkContext, bedFilePath: String, bamFilesPath: String, minMedian: Double = 1.0, svd: Int = 12)
  extends Serializable with FileUtils with ConvertionUtils {

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
   * RDD of (regionId, (sampleId, coverage)) containing coverage.
   */
  def coverage: RDD[(Int, Iterable[(Int, Int)])] = {
    val counter = new CoverageCounter(sc, bedFile, reads)
    coverageToRegionCoverage(counter.calculateCoverage)
  }

  /**
   * RDD of (regionId, (sampleId, rpkm)) containing RPKM values.
   */
  def rpkms(coverage: RDD[(Int, Iterable[(Int, Int)])]): RDD[(Int, Iterable[(Int, Double)])] = {
    val counter = new RpkmsCounter(sc, reads, bedFile, coverage)
    counter.calculateRpkms
  }

  /**
   * RDD of (regionId, (sampleId, zrpkm)) containing ZRPKM values.
   */
  def zrpkms(rpkms: RDD[(Int, Iterable[(Int, Double)])]): RDD[(Int, Iterable[(Int, Double)])] = {
    val counter = new ZrpkmsCounter(samples, rpkms, minMedian)
    counter.calculateZrpkms
  }

  def svd(zrpkms: RDD[(Int, Iterable[(Int, Double)])]) = {
    val counter = new SvdCounter(sc, samples, bedFile, zrpkms)
    counter.calculateSvd
  }

  //  private def removeComponents(vec: org.apache.spark.mllib.linalg.Vector): org.apache.spark.mllib.linalg.Vector = {
  //    val tmp = vec.toArray
  //    (0 until svd) map (tmp.update(_, 0))
  //    Vectors.dense(tmp)
  //  }

}

package pl.edu.pw.elka.cnv.conifer

import htsjdk.samtools.SAMRecord
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.coverage.CoverageCounter
import pl.edu.pw.elka.cnv.rpkm.RpkmsCounter
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
   * Array of (sampleId, samplePath) containing all of the found BAM files.
   */
  val samples: Array[(Int, String)] = scanForSamples(bamFilesPath)

  /**
   * RDD of (sampleId, read) containing all of the reads to be analyzed.
   */
  val reads: RDD[(Int, SAMRecord)] = loadReads(sc, samples)

  /**
   * RDD of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
   */
  val bedFile: RDD[(Int, (Int, Int, Int))] = readBedFile(sc, bedFilePath)

  /**
   * RDD of (regionId, (sampleId, coverage)) containing coverage.
   */
  val coverage: RDD[(Int, Iterable[(Int, Int)])] = {
    val counter = new CoverageCounter(sc, bedFile, reads)
    coverageToRegionCoverage(counter.calculateCoverage)
  }

  /**
   * RDD of (regionId, (sampleId, rpkm)) containing RPKM values.
   */
  val rpkms: RDD[(Int, Iterable[(Int, Double)])] = {
    val counter = new RpkmsCounter(reads, bedFile, coverage)
    counter.calculateRpkms
  }

  /**
   * RDD of (regionId, (sampleId, zrpkm)) containing ZRPKM values.
   */
  val zrpkms: RDD[(Int, Iterable[(Int, Double)])] = {
    val counter = new ZrpkmsCounter(rpkms, minMedian)
    counter.calculateZrpkms
  }

  def calculateSVD = {
    val rows = zrpkms.sortByKey().map {
      case (_, samplesZRPKM) => Vectors.dense(samplesZRPKM.toArray.sorted.map(_._2))
    }

    val svd = new RowMatrix(rows).computeSVD(samples.size, computeU = true)
    val newS = removeComponents(svd.s)

    svd.U.multiply(Matrices.diag(newS)).multiply(svd.V)
  }

  private def removeComponents(vec: org.apache.spark.mllib.linalg.Vector): org.apache.spark.mllib.linalg.Vector = {
    val tmp = vec.toArray
    (0 until svd) map (tmp.update(_, 0))
    Vectors.dense(tmp)
  }

}

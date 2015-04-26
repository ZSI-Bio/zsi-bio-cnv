package pl.edu.pw.elka.cnv.conifer

import htsjdk.samtools.SAMRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.coverage.CoverageCounter
import pl.edu.pw.elka.cnv.utils.{ConvertionUtils, FileUtils}

/**
 * Main class for CoNIFER algorithm.
 *
 * @param sc Apache Spark context.
 * @param bedFilePath Path to folder containing BED file.
 * @param bamFilesPath Path to folder containing BAM files.
 */
class Conifer(@transient sc: SparkContext, bedFilePath: String, bamFilesPath: String)
  extends Serializable with FileUtils with ConvertionUtils {

  /**
   * Array of (sampleId, samplePath) containing all of the found BAM files.
   */
  private val samples: Array[(Int, String)] = scanForSamples(bamFilesPath)

  /**
   * RDD of (sampleId, read) containing all of the reads to be analyzed.
   */
  private val reads: RDD[(Int, SAMRecord)] = loadReads(sc, samples)

  /**
   * RDD of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
   */
  val bedFile: RDD[(Int, (String, Int, Int))] = readBedFile(sc, bedFilePath)

  /**
   * RDD of (regionId, (sampleId, coverage)) containing coverage.
   */
  val coverage: RDD[(Int, Iterable[(Int, Int)])] = {
    val counter = new CoverageCounter(sc, bedFile, reads)
    coverageToRegionCoverage(counter.calculateCoverage)
  }

  //  val RPKMsByExone: RDD[(Long, Iterable[Double])] =
  //    bamFilePaths.map(loadBAMFile).map(getRPKMs).reduce(_ ++ _).groupByKey.cache
  //
  //  val ZRPKMsByExone: RDD[(Long, Iterable[Double])] =
  //    RPKMsByExone mapValues {
  //      rpkms => (rpkms, median(rpkms.toArray), stddev(rpkms.toArray))
  //    } filter {
  //      case (_, (_, median, _)) => median >= minMedian
  //    } flatMap {
  //      case (id, (rpkms, median, stddev)) =>
  //        rpkms.map(rpkm => (id, zrpkm(rpkm, median, stddev)))
  //    } groupByKey() cache()
  //
  //
  //  private def getRPKMs(bamFile: RDD[SAMRecord]): RDD[(Long, Double)] = {
  //    val total = bamFile.count.toDouble
  //    getCoverage(bamFile) map {
  //      case ((id, start, stop), count) =>
  //        (id, rpkm(count, stop - start, total))
  //    }
  //  }
  //
  //  def getSVD(chr: String) = {
  //    val exons = exonsByChromosome.value(chr)
  //    val (minId, maxId) = (exons.minBy(_._1)._1, exons.maxBy(_._1)._1)
  //
  //    val rows = ZRPKMsByExone filter {
  //      case (id, _) => id >= minId && id <= maxId
  //    } map {
  //      case (_, zrpkms) => Vectors.dense(zrpkms.toArray)
  //    }
  //
  //    System.console().printf("Rows: " + rows.count + "\n")
  //    System.console().printf("Cols: " + rows.first.size + "\n")
  //
  //    //TODO calculation of SVD
  //  }

}

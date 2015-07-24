package pl.edu.pw.elka.cnv.conifer

import htsjdk.samtools.SAMRecord
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.coverage.CoverageCounter
import pl.edu.pw.elka.cnv.rpkm.RpkmsCounter
import pl.edu.pw.elka.cnv.utils.{ConvertionUtils, FileUtils}
import pl.edu.pw.elka.cnv.zrpkm.ZrpkmsCounter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

  def calculateSVD(zrpkms: RDD[(Int, Iterable[(Int, Double)])]) = {
    val regionsMap = test(bedFile)
    val svdMap = new mutable.HashMap[Int, ArrayBuffer[linalg.Vector]]

    zrpkms.collect.foreach {
      case (regionId, sampleZrpkms) =>
        val row = sampleZrpkms.toArray.sorted
        val ind = regionsMap(regionId)
        if (!svdMap.contains(ind))
          svdMap(ind) = new ArrayBuffer[linalg.Vector]
        svdMap(ind) += Vectors.dense(row.map(_._2))
    }

//    val result = svdMap map {
//      case (chr, rows) =>
//        new RowMatrix(sc.makeRDD(rows)).computeSVD(samples.size)
//    } map (_.s.toArray.mkString("\t")) map (_.mkString("\n"))
//
//    System.console().printf(result + "\n")
    //val newS = removeComponents(svd.s)
    //svd.U.multiply(Matrices.diag(newS)).multiply(svd.V)
  }


  private def test(bedFile: RDD[(Int, (Int, Int, Int))]) = {
    val result = new mutable.HashMap[Int, Int]
    for ((regionId, (chr, _, _)) <- bedFile.collect) {
      result(regionId) = chr
    }
    result
  }

  //  private def removeComponents(vec: org.apache.spark.mllib.linalg.Vector): org.apache.spark.mllib.linalg.Vector = {
  //    val tmp = vec.toArray
  //    (0 until svd) map (tmp.update(_, 0))
  //    Vectors.dense(tmp)
  //  }

}

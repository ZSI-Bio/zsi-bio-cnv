package pl.edu.pw.elka.cnv.conifer

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.coverage.CoverageCounter
import pl.edu.pw.elka.cnv.model.CNVRecord
import pl.edu.pw.elka.cnv.utils.FileUtils

import scala.collection.mutable

/**
 * Created by mariusz-macbook on 20/11/15.
 */
class Coverage(@transient sc: SparkContext, bedFilePath: String, bamFilesPath: String)
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

  def calculate: RDD[(Int, Iterable[(Int, Int)])] = {
    val counter = new CoverageCounter(sc, bedFile, reads)
    coverageToRegionCoverage(counter.calculateReadCoverage)
  }

}

package pl.edu.pw.elka.cnv.utils

import java.io.File

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.seqdoop.hadoop_bam.{BAMInputFormat, SAMRecordWritable}

import scala.io.Source

/**
 * Created by mariusz-macbook on 26/04/15.
 *
 * Trait for scanning and loading data from BED and BAM files.
 */
trait FileUtils extends ConvertionUtils {

  /**
   * Method for scanning for BAM files under given path.
   *
   * @param path Path to folder containing BAM files.
   * @return Map of (sampleId, samplePath) containing all of the found BAM files.
   */
  def scanForSamples(path: String): Map[Int, String] =
    new File(path).listFiles.filter(
      _.getName.endsWith(".bam")
    ).zipWithIndex.map {
      case (file, index) =>
        (index, file.getPath)
    } toMap

  /**
   * Method for loading all of the samples into single RDD.
   *
   * @param sc Apache Spark context.
   * @param samples Map of (sampleId, samplePath) containing all of the samples to be analyzed.
   * @return RDD of (sampleId, read) containing all of the reads to be analyzed.
   */
  def loadReads(sc: SparkContext, samples: Map[Int, String]): RDD[(Int, SAMRecord)] =
    samples.foldLeft(sc.parallelize[(Int, SAMRecord)](Seq())) {
      case (acc, (sampleId, samplePath)) => acc union {
        sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](samplePath) map {
          read => (sampleId, read._2.get)
        }
      }
    }

  /**
   * Method for loading data from BED file.
   *
   * @param path Path to folder containing BED file.
   * @return Array of (regionId, chr, start, end) containing all of the regions to be analyzed.
   */
  def readBedFile(path: String): Array[(Int, Int, Int, Int)] =
    Source.fromFile(path).getLines.zipWithIndex map {
      case (line, regionId) => line.split("\t") match {
        case Array(chr, start, end, _*) =>
          (regionId.toInt, chrStrToInt(chr), start.toInt, end.toInt)
      }
    } toArray

}

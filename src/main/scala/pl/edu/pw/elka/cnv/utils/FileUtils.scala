package pl.edu.pw.elka.cnv.utils

import java.io.File

import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.AlignmentRecord
import org.seqdoop.hadoop_bam.{BAMInputFormat, SAMRecordWritable}
import pl.edu.pw.elka.cnv.model.{AlignmentRecordAdapter, CNVRecord, SAMRecordAdapter}

import scala.collection.mutable
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
      file => file.getName.endsWith(".bam")
        || file.getName.endsWith(".adam")
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
  def loadReads(sc: SparkContext, samples: Map[Int, String]): RDD[(Int, CNVRecord)] =
    samples.foldLeft(sc.parallelize[(Int, CNVRecord)](Seq())) {
      case (acc, (sampleId, samplePath)) => acc union {
        samplePath match {
          case bam if samplePath.endsWith(".bam") =>
            sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](samplePath) map {
              read => (sampleId, new SAMRecordAdapter(read._2.get))
            }
          case adam if samplePath.endsWith(".adam") =>
            new ADAMContext(sc).loadParquet[AlignmentRecord](samplePath) map {
              read => (sampleId, new AlignmentRecordAdapter(read))
            }
        }
      }
    }

  /**
   * Method for loading data from BED file.
   *
   * @param path Path to folder containing BED file.
   * @return Map of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
   */
  def readBedFile(path: String): mutable.HashMap[Int, (Int, Int, Int)] = {
    val result = new mutable.HashMap[Int, (Int, Int, Int)]
    Source.fromFile(path).getLines.zipWithIndex foreach {
      case (line, regionId) => line.split("\t") match {
        case Array(chr, start, end, _*) =>
          result(regionId.toInt) = ((chrStrToInt(chr), start.toInt, end.toInt))
      }
    }
    result
  }

}

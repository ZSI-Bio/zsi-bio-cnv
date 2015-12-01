package pl.edu.pw.elka.cnv.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.AlignmentRecord
import org.seqdoop.hadoop_bam.{BAMInputFormat, SAMRecordWritable}
import pl.edu.pw.elka.cnv.model.{AlignmentRecordAdapter, CNVRecord, SAMRecordAdapter}
import pl.edu.pw.elka.cnv.utils.ConversionUtils.chrStrToInt

import scala.collection.mutable

/**
 * Object containing methods for scanning and loading data from BED, Interval, BAM and ADAM files.
 */
object FileUtils {

  /**
   * Method for scanning for BAM or ADAM files under given path.
   *
   * @param path Path to folder containing BAM or ADAM files.
   * @return Map of (sampleId, samplePath) containing all of the found BAM or ADAM files.
   */
  def scanForSamples(path: String): Map[Int, String] = {
    val fs = FileSystem.get(new Configuration())
    fs.listStatus(new Path(path), new PathFilter {
      override def accept(path: Path): Boolean =
        path.getName match {
          case bam if bam.endsWith(".bam") => true
          case adam if adam.endsWith(".adam") => true
          case _ => false
        }
    }).zipWithIndex map {
      case (file, index) =>
        (index, file.getPath.toString)
    } toMap
  }

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
   * Method for loading data from BED or Interval file.
   *
   * @param path Path to BED or Interval file.
   * @return Map of (regionId, (chr, start, end)) containing all of the regions to be analyzed.
   */
  def readRegionFile(sc: SparkContext, path: String): mutable.HashMap[Int, (Int, Int, Int)] = {

    def readBedFile: mutable.HashMap[Int, (Int, Int, Int)] = {
      val result = new mutable.HashMap[Int, (Int, Int, Int)]
      sc.textFile(path).collect.zipWithIndex foreach {
        case (line, regionId) => line.split("\t") match {
          case Array(chr, start, end, _*) =>
            result(regionId) = ((chrStrToInt(chr), start.toInt, end.toInt))
        }
      }
      result
    }

    def readIntervalFile: mutable.HashMap[Int, (Int, Int, Int)] = {
      val result = new mutable.HashMap[Int, (Int, Int, Int)]
      sc.textFile(path).collect.zipWithIndex foreach {
        case (line, regionId) => line.split(":") match {
          case Array(chr, coords) => coords.split("-") match {
            case Array(start, end) =>
              result(regionId) = ((chrStrToInt(chr), start.toInt, end.toInt))
          }
        }
      }
      result
    }

    path match {
      case bed if path.endsWith(".bed") => readBedFile
      case interval if path.endsWith(".interval_list") => readIntervalFile
    }
  }

}

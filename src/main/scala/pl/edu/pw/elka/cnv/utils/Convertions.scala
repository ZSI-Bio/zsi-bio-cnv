package pl.edu.pw.elka.cnv.utils

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait Convertions {

  def scanForSamples(path: String): Array[(Int, String)] =
    new File(path).listFiles.filter(
      file => file.getName.endsWith(".bam")
    ).zipWithIndex.map {
      case (file, index) =>
        (index, file.getPath)
    }

  def readBedFile(sc: SparkContext, path: String): RDD[(Int, (String, Int, Int))] =
    sc.textFile(path).zipWithIndex map {
      case (line, index) => line.split("\t") match {
        case Array(chr, start, end, _*) =>
          (index.toInt, (chr, start.toInt, end.toInt))
      }
    }

  def bedFileToFeaturesMap(bedFile: RDD[(Int, (String, Int, Int))]): mutable.HashMap[String, Array[ArrayBuffer[(Int, Int, Int)]]] = {
    val result = new mutable.HashMap[String, Array[ArrayBuffer[(Int, Int, Int)]]]
    for ((index, (chr, start, end)) <- bedFile.collect) {
      if (!result.contains(chr))
        result(chr) = new Array[ArrayBuffer[(Int, Int, Int)]](25000)
      val startId = start / 10000
      if (result(chr)(startId) == null)
        result(chr)(startId) = new ArrayBuffer[(Int, Int, Int)]
      result(chr)(startId) += ((index, start, end))
    }
    result
  }

  def encodeCoverageId(sampleId: Int, featureId: Int): Long =
    sampleId * 1000000000L + featureId

  def decodeCoverageId(coverageId: Long): (Int, Int) =
    ((coverageId / 1000000000L).toInt, (coverageId % 1000000000L).toInt)

}

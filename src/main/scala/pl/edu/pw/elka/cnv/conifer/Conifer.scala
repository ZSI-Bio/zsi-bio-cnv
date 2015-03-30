package pl.edu.pw.elka.cnv.conifer

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.seqdoop.hadoop_bam.SAMRecordWritable

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Conifer(probesFile: RDD[String], bamFile: RDD[(LongWritable, SAMRecordWritable)]) extends Serializable {

  val probes: Array[(String, Long, Long)] =
    probesFile map {
      line => line.split("\t") match {
        case Array(chr, start, stop, _*) =>
          (chr, start.toLong, stop.toLong)
      }
    } collect

  lazy val exonsMap: mutable.HashMap[String, ArrayBuffer[(Long, Long, Long)]] = {
    val result = new mutable.HashMap[String, ArrayBuffer[(Long, Long, Long)]]
    var counter = 1

    probes map {
      case (chr, start, stop) => {
        if (!result.contains(chr))
          result(chr) = new ArrayBuffer[(Long, Long, Long)]()
        result(chr) += ((counter, start, stop))
        counter = counter + 1
      }
    }

    result
  }

  lazy val reads: RDD[SAMRecord] = bamFile.map(read => read._2.get)

  lazy val coverageWithLen: RDD[((Long, Long), Long)] =
    reads.mapPartitions(partition => {
      for {
        read <- partition
        (id, start, stop) <- exonsMap(read.getReferenceName)
        if (read.getAlignmentStart >= start && read.getAlignmentStart <= stop)
      } yield ((id, stop - start), 1L)
    }).reduceByKey(_ + _)

  def calculateRPKMs(): RDD[(Long, Long, Long)] = {
    val totalReads = reads.count
    coverageWithLen map {
      case ((id, len), count) =>
        (id, count, 1000000000 * count / len / totalReads)
    }
  }

}

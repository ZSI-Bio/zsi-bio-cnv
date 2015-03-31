package pl.edu.pw.elka.cnv.conifer

import htsjdk.samtools.SAMRecord
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Conifer(probes: Array[(String, Long, Long)], bamFiles: Array[RDD[SAMRecord]]) extends Serializable {

  private val exons: mutable.HashMap[String, ArrayBuffer[(Long, Long, Long)]] = {
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

  def calculateRPKMs: Array[RDD[(Long, Long, Long, Long, Float)]] =
    bamFiles.map(rpkm)

  private def rpkm(bamFile: RDD[SAMRecord]): RDD[(Long, Long, Long, Long, Float)] = {
    val totalReads = bamFile.count.toFloat
    coverage(bamFile) map {
      case ((id, start, stop), count) =>
        (id, start, stop, count, (1000000000 * count) / (stop - start) / totalReads)
    }
  }

  private def coverage(bamFile: RDD[SAMRecord]): RDD[((Long, Long, Long), Long)] =
    bamFile.mapPartitions(partition => {
      for {
        read <- partition
        (id, start, stop) <- exons(read.getReferenceName)
        if (read.getAlignmentStart >= start && read.getAlignmentStart <= stop)
      } yield ((id, start, stop), 1L)
    }).reduceByKey(_ + _)

}

package pl.edu.pw.elka.cnv.conifer

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.seqdoop.hadoop_bam.{BAMInputFormat, SAMRecordWritable}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Conifer(@transient sc: SparkContext, probesFilePath: String, bamFilePaths: Array[String], minMedian: Double, k: Int) extends Serializable with CNVUtils {

  private val probes: Array[(String, Long, Long)] =
    sc.textFile(probesFilePath).map(
      line => line.split("\t") match {
        case Array(chr, start, stop, _*) =>
          (chr, start.toLong, stop.toLong)
      }
    ).collect

  private val exonsByChromosome: Broadcast[mutable.HashMap[String, ArrayBuffer[(Long, Long, Long)]]] = {
    val result = new mutable.HashMap[String, ArrayBuffer[(Long, Long, Long)]]
    var counter = 1

    probes foreach {
      case (chr, start, stop) => {
        if (!result.contains(chr))
          result(chr) = new ArrayBuffer[(Long, Long, Long)]()
        result(chr) += ((counter, start, stop))
        counter = counter + 1
      }
    }

    sc.broadcast(result)
  }

  val RPKMsByExone: RDD[(Long, Iterable[Double])] =
    bamFilePaths.map(loadBAMFile).map(getRPKMs).reduce(_ ++ _).groupByKey.cache

  val ZRPKMsByExone: RDD[(Long, Iterable[Double])] =
    RPKMsByExone mapValues {
      rpkms => (rpkms, median(rpkms.toArray), stddev(rpkms.toArray))
    } filter {
      case (_, (_, median, _)) => median >= minMedian
    } flatMap {
      case (id, (rpkms, median, stddev)) =>
        rpkms.map(rpkm => (id, zrpkm(rpkm, median, stddev)))
    } groupByKey() cache()

  private def loadBAMFile(path: String): RDD[SAMRecord] =
    sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](path) map {
      read => read._2.get
    }

  private def getRPKMs(bamFile: RDD[SAMRecord]): RDD[(Long, Double)] = {
    val total = bamFile.count.toDouble
    getCoverage(bamFile) map {
      case ((id, start, stop), count) =>
        (id, rpkm(count, stop - start, total))
    }
  }

  private def getCoverage(bamFile: RDD[SAMRecord]): RDD[((Long, Long, Long), Long)] =
    bamFile.mapPartitions(partition =>
      for {
        read <- partition
        (id, start, stop) <- exonsByChromosome.value(read.getReferenceName)
        if (read.getAlignmentStart >= start && read.getAlignmentStart <= stop)
      } yield ((id, start, stop), 1L)
    ).reduceByKey(_ + _)

  def getSVD(chr: String) = {
    val exons = exonsByChromosome.value(chr)
    val (minId, maxId) = (exons.minBy(_._1)._1, exons.maxBy(_._1)._1)

    val rows = ZRPKMsByExone filter {
      case (id, _) => id >= minId && id <= maxId
    } map {
      case (_, zrpkms) => Vectors.dense(zrpkms.toArray)
    }

    System.console().printf("Rows: " + rows.count + "\n")
    System.console().printf("Cols: " + rows.first.size + "\n")

    //TODO calculation of SVD
  }

}

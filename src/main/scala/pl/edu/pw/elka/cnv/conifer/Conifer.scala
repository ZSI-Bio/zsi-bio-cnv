package pl.edu.pw.elka.cnv.conifer

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.seqdoop.hadoop_bam.{BAMInputFormat, SAMRecordWritable}
import pl.edu.pw.elka.cnv.coverage.CoverageCounter
import pl.edu.pw.elka.cnv.utils.Convertions

class Conifer(@transient sc: SparkContext, bedFilePath: String, bamFilesPath: String)
  extends Serializable with Convertions {

  private val samples: Array[(Int, String)] = scanForSamples(bamFilesPath)
  val bedFile: RDD[(Int, (String, Int, Int))] = readBedFile(sc, bedFilePath)

  private val reads: RDD[(Int, SAMRecord)] =
    samples.foldLeft(sc.parallelize[(Int, SAMRecord)](Seq())) {
      case (acc, (sampleId, sampleName)) => acc union {
        sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](sampleName) map {
          read => (sampleId, read._2.get)
        }
      }
    }

  val coverage: RDD[(Int, Iterable[(Int, Int)])] =
    new CoverageCounter(sc, bedFile, reads).genCoverage map {
      case (coverageId, coverage) => (decodeCoverageId(coverageId), coverage)
    } map {
      case ((sampleId, featureId), coverage) => (featureId, (sampleId, coverage))
    } groupByKey

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

package pl.edu.pw.elka.cnv.conifer

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.seqdoop.hadoop_bam.{BAMInputFormat, SAMRecordWritable}

object Test {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("CNV")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val probesFile = loadProbesFile(sc,
      "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/probes.txt")

    val bamFile1 = loadBAMFile(sc,
      "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/NA12878.chrom11.ILLUMINA.bwa.CEU.low_coverage.20121211.bam")

    val bamFile2 = loadBAMFile(sc,
      "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/NA12878.chrom11.ILLUMINA.bwa.CEU.low_coverage.20121211.bam")

    val bamFile3 = loadBAMFile(sc,
      "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/NA12878.chrom11.ILLUMINA.bwa.CEU.low_coverage.20121211.bam")

    val bamFiles = Array(bamFile1, bamFile2, bamFile3)

    val conifer = new Conifer(probesFile, bamFiles)
    val rpkms = conifer.calculateRPKMs

    rpkms.map(saveRPKMs)
  }

  private def loadProbesFile(sc: SparkContext, path: String): Array[(String, Long, Long)] =
    sc.textFile(path) map {
      line => line.split("\t") match {
        case Array(chr, start, stop, _*) =>
          (chr, start.toLong, stop.toLong)
      }
    } collect

  private def loadBAMFile(sc: SparkContext, path: String): RDD[SAMRecord] =
    sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](path) map {
      read => read._2.get
    }

  private def saveRPKMs(rpkms: RDD[(Long, Long, Long, Long, Float)]) = {
    val path = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/rpkms.txt"
    maybeRemoveDir(path)
    rpkms.saveAsTextFile(path)
  }

  private def maybeRemoveDir(path: String) = {
    val fs = FileSystem.get(new Configuration())
    if (fs.exists(new Path(path)))
      fs.delete(new Path(path), true)
  }

}

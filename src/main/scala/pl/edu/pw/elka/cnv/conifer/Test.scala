package pl.edu.pw.elka.cnv.conifer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("CNV")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val probesFilePath = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/probes.txt"

    val bamFilePaths = Array(
      "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/NA12878.chrom11.ILLUMINA.bwa.CEU.low_coverage.20121211.bam",
      "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/NA12878.chrom11.ILLUMINA.bwa.CEU.low_coverage.20121211.bam",
      "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/NA12878.chrom11.ILLUMINA.bwa.CEU.low_coverage.20121211.bam").take(1)

    val conifer = new Conifer(sc, probesFilePath, bamFilePaths)
    val rpkms = conifer.calculateRPKMs

    saveRPKMs(rpkms)
  }

  private def saveRPKMs(rpkms: RDD[(Long, Iterable[Float])]) = {
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

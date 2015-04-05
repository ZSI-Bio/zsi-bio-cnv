package pl.edu.pw.elka.cnv.conifer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("CNV")
      .setMaster("local[2]")
      .set("spark.driver.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    val probesFilePath = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/probes.txt"

    val bamFilePaths = Array(
      "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/NA12878.chrom11.ILLUMINA.bwa.CEU.low_coverage.20121211.bam",
      "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/NA12878.chrom11.ILLUMINA.bwa.CEU.low_coverage.20121211.bam",
      "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/NA12878.chrom11.ILLUMINA.bwa.CEU.low_coverage.20121211.bam").take(1)

    val conifer = new Conifer(sc, probesFilePath, bamFilePaths, 1D, 1)

    saveRPKMs(conifer.RPKMsByExone)
    saveZRPKMs(conifer.ZRPKMsByExone)

    conifer.getSVD("11")
  }

  private def saveRPKMs(rpkms: RDD[(Long, Iterable[Double])]) = {
    val path = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/rpkms.txt"
    maybeRemoveDir(path)
    rpkms.saveAsTextFile(path)
  }

  private def saveZRPKMs(zrpkms: RDD[(Long, Iterable[Double])]) = {
    val path = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/zrpkms.txt"
    maybeRemoveDir(path)
    zrpkms.saveAsTextFile(path)
  }

  private def maybeRemoveDir(path: String) = {
    val fs = FileSystem.get(new Configuration())
    if (fs.exists(new Path(path)))
      fs.delete(new Path(path), true)
  }

}

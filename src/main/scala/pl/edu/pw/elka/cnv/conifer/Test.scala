package pl.edu.pw.elka.cnv.conifer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("CNV")
      .setMaster("local[*]")
      .set("spark.driver.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    val bedFilePath = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/probes.txt"
    val bamFilesPath = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/"

    val conifer = new Conifer(sc, bedFilePath, bamFilesPath)

    saveBedFile(conifer.bedFile.sortByKey())
    saveCoverage(conifer.coverage.sortByKey())

    //    saveRPKMs(conifer.RPKMsByExone)
    //    saveZRPKMs(conifer.ZRPKMsByExone)
    //    conifer.getSVD("11")
  }

  private def saveBedFile(bedFile: RDD[(Int, (String, Int, Int))]) = {
    val path = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/bedFile.txt"
    maybeRemoveDir(path)
    bedFile.saveAsTextFile(path)
  }

  private def saveCoverage(coverage: RDD[(Int, Iterable[(Int, Int)])]) = {
    val path = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/coverage.txt"
    maybeRemoveDir(path)
    coverage.saveAsTextFile(path)
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

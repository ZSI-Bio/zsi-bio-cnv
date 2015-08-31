package pl.edu.pw.elka.cnv.conifer

import org.apache.commons.math3.linear.RealMatrix
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

    val bedFilePath = "/Users/mariusz-macbook/Downloads/ZSI-Bio/Tools/conifer_v0.2.2/exomes_data/20120518.consensus.annotation.txt"
    val bamFilesPath = "/Users/mariusz-macbook/Downloads/ZSI-Bio/Tools/conifer_v0.2.2/exomes_data"

    val start = System.currentTimeMillis
    val conifer = new Conifer(sc, bedFilePath, bamFilesPath)
    val loadingTime = System.currentTimeMillis

    val coverage = conifer.coverage
    saveCoverage(coverage)
    val coverageTime = System.currentTimeMillis

    val rpkms = conifer.rpkms(coverage)
    saveRpkms(rpkms)
    val rpkmTime = System.currentTimeMillis

    val zrpkms = conifer.zrpkms(rpkms)
    saveZrpkms(zrpkms)
    val zrpkmTime = System.currentTimeMillis

    val matrices = conifer.svd(zrpkms)
    saveMatrices(matrices)
    val svdTime = System.currentTimeMillis

    val calls = conifer.call(matrices)
    saveCalls(calls)
    val callTime = System.currentTimeMillis

    System.console().printf(
      "Loading: " + (loadingTime - start) + " ms\n" +
        "Coverage: " + (coverageTime - loadingTime) + " ms\n" +
        "RPKM: " + (rpkmTime - coverageTime) + " ms\n" +
        "ZRPKM: " + (zrpkmTime - rpkmTime) + " ms\n" +
        "SVD: " + (svdTime - zrpkmTime) + " ms\n" +
        "Calling: " + (callTime - svdTime) + " ms\n"
    )

    System.console().printf(
      "Loading: " + (loadingTime - start) + " ms\n" +
        "Coverage & RPKM: " + (rpkmTime - loadingTime) + " ms\n" +
        "ZRPKM & SVD: " + (svdTime - rpkmTime) + " ms\n" +
        "Calling: " + (callTime - svdTime) + " ms\n"
    )

  }

  private def saveCoverage(coverage: RDD[(Int, Iterable[(Int, Int)])]) = {
    val path = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/results/coverage.txt"
    maybeRemoveDir(path)
    coverage.saveAsTextFile(path)
  }

  private def saveRpkms(rpkms: RDD[(Int, Iterable[(Int, Double)])]) = {
    val path = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/results/rpkms.txt"
    maybeRemoveDir(path)
    rpkms.saveAsTextFile(path)
  }

  private def saveZrpkms(zrpkms: RDD[(Int, Iterable[(Int, Double)])]) = {
    val path = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/results/zrpkms.txt"
    maybeRemoveDir(path)
    zrpkms.saveAsTextFile(path)
  }

  private def saveMatrices(matrices: RDD[(Int, RealMatrix)]) = {
    val path = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/results/matrices.txt"
    maybeRemoveDir(path)
    matrices.saveAsTextFile(path)
  }

  private def saveCalls(calls: RDD[(Int, Array[(Int, Int, Int, String)])]) = {
    val path = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/results/calls.txt"
    maybeRemoveDir(path)
    calls.saveAsTextFile(path)
  }

  private def maybeRemoveDir(path: String) = {
    val fs = FileSystem.get(new Configuration())
    if (fs.exists(new Path(path)))
      fs.delete(new Path(path), true)
  }

}

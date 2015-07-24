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

    val bedFilePath = "/Users/mariusz-macbook/Downloads/ZSI-Bio/Tools/conifer_v0.2.2/exomes_data/20120518.consensus.annotation.txt"
    val bamFilesPath = "/Users/mariusz-macbook/Downloads/ZSI-Bio/Tools/conifer_v0.2.2/exomes_data"

    val start = System.currentTimeMillis

    val conifer = new Conifer(sc, bedFilePath, bamFilesPath)
    val loadingTime = System.currentTimeMillis

    val coverage = conifer.coverage
    val coverageTime = System.currentTimeMillis

    val rpkms = conifer.rpkms(coverage)
    val rpkmTime = System.currentTimeMillis

    val zrpkms = conifer.zrpkms(rpkms)
    val zrpkmTime = System.currentTimeMillis

    conifer.calculateSVD(zrpkms)
    val svdTime = System.currentTimeMillis

    System.console().printf(
      "Loading: " + (loadingTime - start) + " ms\n" +
      "Coverage: " + (coverageTime - loadingTime) + " ms\n" +
      "RPKM: " + (rpkmTime - coverageTime) + " ms\n" +
      "ZRPKM: " + (zrpkmTime - rpkmTime) + " ms\n" +
      "SVD: " + (svdTime - zrpkmTime) + " ms\n"
    )

    //    saveBedFile(conifer.bedFile.sortByKey())
    //    saveCoverage(conifer.coverage.sortByKey())
    //    saveRpkms(conifer.rpkms.sortByKey())
    //    saveZrpkms(conifer.zrpkms.sortByKey())

    //    System.console().printf("Samples: " + conifer.samples.size + "\n")
    //    System.console().printf("Reads: " + conifer.reads.count + "\n")
    //    System.console().printf("BED: " + conifer.bedFile.count + "\n")
    //    System.console().printf("Coverage: " + conifer.coverage.count + "\n")
    //    System.console().printf("RPKMS: " + conifer.rpkms.count + "\n")
    //    System.console().printf("ZRPKMS: " + conifer.zrpkms.count + "\n")
    //    System.console().printf("Matrix rows: " + mat.numRows + "\n")
    //    System.console().printf("Matrix cols: " + mat.numCols + "\n")

  }

  private def saveBedFile(bedFile: RDD[(Int, (Int, Int, Int))]) = {
    val path = "/Users/mariusz-macbook/IdeaProjects/zsi-bio-cnv/resources/results/bedFile.txt"
    maybeRemoveDir(path)
    bedFile.saveAsTextFile(path)
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

  private def maybeRemoveDir(path: String) = {
    val fs = FileSystem.get(new Configuration())
    if (fs.exists(new Path(path)))
      fs.delete(new Path(path), true)
  }

}

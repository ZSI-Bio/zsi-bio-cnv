package pl.edu.pw.elka.cnv.conifer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    args.head match {
      case "Coverage" => runCoverage(args.tail, sc)
      case "DepthOfCoverage" => runDepthOfCoverage(args.tail, sc)
      case "Conifer" => runConifer(args.tail, sc)
    }
  }

  private def runCoverage(args: Array[String], sc: SparkContext): Unit = {
    val coverage = new Coverage(sc, args(0), args(1))
    val result = coverage.calculate
    saveResult(args(2), result)
  }

  private def runDepthOfCoverage(args: Array[String], sc: SparkContext): Unit = {
    val depthOfCoverage = new DepthOfCoverage(sc, args(0), args(1))
    val result = depthOfCoverage.calculate
    saveResult(args(2), result)
  }

  private def runConifer(args: Array[String], sc: SparkContext): Unit = {
    val conifer = new Conifer(sc, args(0), args(1), args(2).toDouble, args(3).toInt, args(4).toDouble)
    val result = conifer.calculate
    saveResult(args(5), result)
  }

  private def saveResult(path: String, result: RDD[_]): Unit = {
    maybeRemoveDir(new Path(path))
    result.saveAsTextFile(path)
  }

  private def maybeRemoveDir(path: Path): Unit = {
    val fs = FileSystem.get(new Configuration())
    if (fs.exists(path))
      fs.delete(path, true)
  }

}

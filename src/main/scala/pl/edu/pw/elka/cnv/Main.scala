package pl.edu.pw.elka.cnv

import java.io.{OutputStreamWriter, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import pl.edu.pw.elka.cnv.application.{Conifer, Coverage, DepthOfCoverage}

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
    val metricsListener = initializeMetrics(sc)

    val coverage = new Coverage(sc, args(0), args(1))
    val result = coverage.calculate
    saveResult(args(2), result)

    printMetrics(metricsListener)
  }

  private def runDepthOfCoverage(args: Array[String], sc: SparkContext): Unit = {
    val metricsListener = initializeMetrics(sc)

    val depthOfCoverage = new DepthOfCoverage(sc, args(0), args(1))
    val result = depthOfCoverage.calculate
    saveResult(args(2), result)

    printMetrics(metricsListener)
  }

  private def runConifer(args: Array[String], sc: SparkContext): Unit = {
    val metricsListener = initializeMetrics(sc)

    val conifer = new Conifer(sc, args(0), args(1), args(2).toDouble, args(3).toInt, args(4).toDouble)
    val result = conifer.calculate
    saveResult(args(5), result)

    printMetrics(metricsListener)
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

  private def initializeMetrics(sc: SparkContext): MetricsListener = {
    Metrics.initialize(sc)
    val metricsListener = new MetricsListener(new RecordedMetrics)
    sc.addSparkListener(metricsListener)
    metricsListener
  }

  private def printMetrics(metricsListener: MetricsListener): Unit = {
    val writer = new PrintWriter(new OutputStreamWriter(System.out))
    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.close
  }

}

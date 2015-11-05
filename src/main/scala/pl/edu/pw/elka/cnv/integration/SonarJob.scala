package pl.edu.pw.elka.cnv.integration

import java.util.UUID

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.jobserver.{NamedRddSupport, SparkJob}

/**
 * Created by mariusz-macbook on 05/11/15.
 */
trait SonarJob extends SparkJob with NamedRddSupport {

  def runSingleJob(sc: SparkContext, jobConfig: Config): RDD[_]

  def runPipelineFirstJob(sc: SparkContext, jobConfig: Config): RDD[_]

  def runPipelineInnerJob(sc: SparkContext, jobConfig: Config, rdd: RDD[_]): RDD[_]

  def runPipelineLastJob(sc: SparkContext, jobConfig: Config, rdd: RDD[_]): RDD[_]

  final override def runJob(sc: SparkContext, jobConfig: Config): String = {
    val jobType = jobConfig.getString("type")
    jobType match {
      case "FIRST" => saveNamedRddAndReturnName(runPipelineFirstJob(sc, jobConfig))
      case "INNER" => saveNamedRddAndReturnName(runPipelineInnerJob(sc, jobConfig, getRddByName(jobConfig)))
      case "LAST" => saveRddAsFile(jobConfig, runPipelineLastJob(sc, jobConfig, getRddByName(jobConfig)))
      case _ => saveRddAsFile(jobConfig, runSingleJob(sc, jobConfig))
    }
  }

  def getRddByName(jobConfig: Config): RDD[_] = {
    val rddName = jobConfig.getString("rdd")
    this.namedRdds.get(rddName).get
  }

  def saveNamedRddAndReturnName(rdd: RDD[_]): String = {
    val rddName = UUID.randomUUID().toString
    this.namedRdds.update(rddName, rdd)
    rddName
  }

  def saveRddAsFile(jobConfig: Config, rdd: RDD[_]): String = {
    val outputPath = jobConfig.getString("output")
    rdd.saveAsTextFile(outputPath)
    outputPath
  }

}

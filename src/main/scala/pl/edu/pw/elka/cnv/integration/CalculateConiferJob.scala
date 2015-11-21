package pl.edu.pw.elka.cnv.integration

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.application.Conifer
import spark.jobserver.{SparkJobValid, SparkJobValidation}

/**
 * Created by mariusz-macbook on 05/11/15.
 */
class CalculateConiferJob extends SonarJob {

  override def runSingleJob(sc: C, jobConfig: Config): RDD[_] = {
    val bamFilesPath = jobConfig.getString("bamFilesPath")
    val bedFilePath = jobConfig.getString("bedFilePath")
    val minMedian = jobConfig.getDouble("minMedian")
    val svd = jobConfig.getInt("svd")
    val threshold = jobConfig.getDouble("threshold")

    val conifer = new Conifer(sc, bedFilePath, bamFilesPath, minMedian, svd, threshold)
    conifer.calculate
  }

  override def runPipelineFirstJob(sc: C, jobConfig: Config): RDD[_] =
    throw new UnsupportedOperationException

  override def runPipelineInnerJob(sc: C, jobConfig: Config, rdd: RDD[_]): RDD[_] =
    throw new UnsupportedOperationException

  override def runPipelineLastJob(sc: C, jobConfig: Config, rdd: RDD[_]): RDD[_] =
    throw new UnsupportedOperationException

  override def validate(sc: C, config: Config): SparkJobValidation =
    SparkJobValid

}

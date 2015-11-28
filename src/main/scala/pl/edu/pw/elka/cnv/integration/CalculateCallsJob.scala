package pl.edu.pw.elka.cnv.integration

import com.typesafe.config.Config
import org.apache.commons.math3.linear.RealMatrix
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.caller.Caller
import pl.edu.pw.elka.cnv.utils.FileUtils
import spark.jobserver.{SparkJobValid, SparkJobValidation}

/**
 * Created by mariusz-macbook on 05/11/15.
 */
class CalculateCallsJob extends SonarJob with FileUtils {

  override def runSingleJob(sc: C, jobConfig: Config): RDD[_] =
    throw new UnsupportedOperationException

  override def runPipelineFirstJob(sc: C, jobConfig: Config): RDD[_] =
    throw new UnsupportedOperationException

  override def runPipelineInnerJob(sc: C, jobConfig: Config, rdd: RDD[_]): RDD[_] =
    throw new UnsupportedOperationException

  override def runPipelineLastJob(sc: C, jobConfig: Config, rdd: RDD[_]): RDD[_] = {
    val bedFilePath = jobConfig.getString("bedFilePath")
    val threshold = jobConfig.getDouble("threshold")

    val bedFile = sc.broadcast {
      readRegionFile(sc, bedFilePath)
    }

    val matrices = rdd.asInstanceOf[RDD[(Int, Array[Int], RealMatrix)]]
    val caller = new Caller(bedFile, matrices, threshold)
    caller.call
  }

  override def validate(sc: C, config: Config): SparkJobValidation =
    SparkJobValid

}

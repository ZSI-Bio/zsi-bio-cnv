package pl.edu.pw.elka.cnv.integration

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.zrpkm.ZrpkmsCounter
import spark.jobserver.{SparkJobValid, SparkJobValidation}

/**
 * Created by mariusz-macbook on 05/11/15.
 */
class CalculateZrpkmsJob extends SonarJob {

  override def runSingleJob(sc: C, jobConfig: Config): RDD[_] =
    throw new UnsupportedOperationException

  override def runPipelineFirstJob(sc: C, jobConfig: Config): RDD[_] =
    throw new UnsupportedOperationException

  override def runPipelineInnerJob(sc: C, jobConfig: Config, rdd: RDD[_]): RDD[_] = {
    val rpkms = rdd.asInstanceOf[RDD[(Int, Array[Double])]]
    val minMedian = jobConfig.getDouble("minMedian")

    val counter = new ZrpkmsCounter(rpkms, minMedian)
    counter.calculateZrpkms
  }

  override def runPipelineLastJob(sc: C, jobConfig: Config, rdd: RDD[_]): RDD[_] =
    throw new UnsupportedOperationException

  override def validate(sc: C, config: Config): SparkJobValidation =
    SparkJobValid

}

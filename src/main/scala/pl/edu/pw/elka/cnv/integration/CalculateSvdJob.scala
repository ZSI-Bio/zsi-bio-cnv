package pl.edu.pw.elka.cnv.integration

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.svd.SvdCounter
import pl.edu.pw.elka.cnv.utils.FileUtils
import spark.jobserver.{SparkJobValid, SparkJobValidation}

/**
 * Created by mariusz-macbook on 05/11/15.
 */
class CalculateSvdJob extends SonarJob with FileUtils {

  override def runSingleJob(sc: C, jobConfig: Config): RDD[_] =
    throw new UnsupportedOperationException

  override def runPipelineFirstJob(sc: C, jobConfig: Config): RDD[_] =
    throw new UnsupportedOperationException

  override def runPipelineInnerJob(sc: C, jobConfig: Config, rdd: RDD[_]): RDD[_] = {
    val bedFilePath = jobConfig.getString("bedFilePath")
    val svd = jobConfig.getInt("svd")

    val bedFile = sc.broadcast {
      readBedFile(bedFilePath)
    }

    val zrpkms = rdd.asInstanceOf[RDD[(Int, Array[Double])]]
    val counter = new SvdCounter(bedFile, zrpkms, svd)
    counter.calculateSvd
  }

  override def runPipelineLastJob(sc: C, jobConfig: Config, rdd: RDD[_]): RDD[_] =
    throw new UnsupportedOperationException

  override def validate(sc: C, config: Config): SparkJobValidation =
    SparkJobValid

}

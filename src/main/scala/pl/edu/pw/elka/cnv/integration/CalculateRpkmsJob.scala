package pl.edu.pw.elka.cnv.integration

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.rpkm.RpkmsCounter
import pl.edu.pw.elka.cnv.utils.FileUtils
import spark.jobserver.{SparkJobValid, SparkJobValidation}

/**
 * Created by mariusz-macbook on 05/11/15.
 */
class CalculateRpkmsJob extends SonarJob with FileUtils {

  override def runSingleJob(sc: C, jobConfig: Config): RDD[_] =
    throw new UnsupportedOperationException

  override def runPipelineFirstJob(sc: C, jobConfig: Config): RDD[_] =
    throw new UnsupportedOperationException

  override def runPipelineInnerJob(sc: C, jobConfig: Config, rdd: RDD[_]): RDD[_] = {
    val bamFilesPath = jobConfig.getString("bamFilesPath")
    val bedFilePath = jobConfig.getString("bedFilePath")

    val samples = scanForSamples(bamFilesPath)
    val reads = loadReads(sc, samples)
    val bedFile = sc.broadcast {
      readRegionFile(sc, bedFilePath)
    }

    val coverage = rdd.asInstanceOf[RDD[(Int, Iterable[(Int, Int)])]]
    val counter = new RpkmsCounter(reads, bedFile, coverage)
    counter.calculateRpkms
  }

  override def runPipelineLastJob(sc: C, jobConfig: Config, rdd: RDD[_]): RDD[_] =
    throw new UnsupportedOperationException

  override def validate(sc: C, config: Config): SparkJobValidation =
    SparkJobValid

}

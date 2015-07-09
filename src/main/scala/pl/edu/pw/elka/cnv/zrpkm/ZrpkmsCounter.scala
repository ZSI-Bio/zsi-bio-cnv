package pl.edu.pw.elka.cnv.zrpkm

import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.utils.StatUtils

/**
 * Main class for calculation of ZRPKM values.
 *
 * @param rpkms RDD of (regionId, (sampleId, rpkm)) containing RPKM values for given regions and samples.
 * @param minMedian Minimum value of median - regions with lower median of RPKM values are discarded.
 */
class ZrpkmsCounter(rpkms: RDD[(Int, Iterable[(Int, Double)])], minMedian: Double)
  extends Serializable with StatUtils {

  /**
   * Method for calculation of ZRPKM values based on RPKM values given in class constructor.
   *
   * @return RDD of (regionId, (sampleId, rpkm)) containing calculated ZRPKM values.
   */
  def calculateZrpkms: RDD[(Int, Iterable[(Int, Double)])] = {
    for {
      (regionId, samplesRpkm) <- rpkms
      med = median(samplesRpkm.unzip._2.toSeq)
      if med > minMedian
      std = stddev(samplesRpkm.unzip._2.toSeq)
      (sampleId, rpkm) <- samplesRpkm
    } yield (regionId, (sampleId, zrpkm(rpkm, med, std)))
  } groupByKey

}

package pl.edu.pw.elka.cnv.zrpkm

import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.utils.{CNVUtils, StatUtils}

/**
 * Main class for calculation of ZRPKM values.
 *
 * @param rpkms RDD of (regionId, rpkms) containing RPKM values of given regions.
 * @param minMedian Minimum value of median - regions with lower median of RPKM values are discarded.
 */
class ZrpkmsCounter(rpkms: RDD[(Int, Array[Double])], minMedian: Double)
  extends Serializable with CNVUtils with StatUtils {

  /**
   * Method for calculation of ZRPKM values based on RPKM values given in class constructor.
   *
   * @return RDD of (regionId, zrpkms) containing calculated ZRPKM values.
   */
  def calculateZrpkms: RDD[(Int, Array[Double])] =
    for {
      (regionId, sampleRpkms) <- rpkms

      med = median(sampleRpkms)
      if med >= minMedian
      std = stddev(sampleRpkms)

      sampleZrpkms = sampleRpkms map {
        rpkm => zrpkm(rpkm, med, std)
      }
    } yield (regionId, sampleZrpkms)

}

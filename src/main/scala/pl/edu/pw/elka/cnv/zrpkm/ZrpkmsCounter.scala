package pl.edu.pw.elka.cnv.zrpkm

import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.utils.StatUtils

import scala.collection.mutable

/**
 * Main class for calculation of ZRPKM values.
 *
 * @param rpkms RDD of (regionId, (sampleId, rpkm)) containing RPKM values for given regions and samples.
 * @param minMedian Minimum value of median - regions with lower median of RPKM values are discarded.
 */
class ZrpkmsCounter(samples: Map[Int, String], rpkms: RDD[(Int, Iterable[(Int, Double)])], minMedian: Double)
  extends Serializable with StatUtils {

  /**
   * Sample Ids
   */
  val sampleIds: Iterable[Int] = samples.keys

  /**
   * Method for calculation of ZRPKM values based on RPKM values given in class constructor.
   *
   * @return RDD of (regionId, (sampleId, zrpkm)) containing calculated ZRPKM values.
   */
  def calculateZrpkms: RDD[(Int, Iterable[(Int, Double)])] = {
    for {
      (regionId, sampleRpkms) <- rpkms
      sampleRpkmsWithZeros = fillWithZeros(sampleRpkms)

      med = median(sampleRpkmsWithZeros.unzip._2)
      if med > minMedian
      std = stddev(sampleRpkmsWithZeros.unzip._2)

      (sampleId, rpkm) <- sampleRpkmsWithZeros
    } yield (regionId, (sampleId, zrpkm(rpkm, med, std)))
  } groupByKey

  /**
   * Method that puts zeros in place of no RPKM value.
   *
   * @return Sequence of (sampleId, rpkm) containing RPKM values for given samples.
   */
  private def fillWithZeros(sampleRpkms: Iterable[(Int, Double)]): Seq[(Int, Double)] = {
    val result = new mutable.HashMap[Int, Double]
    sampleIds.foreach(x => result(x) = 0.0)
    sampleRpkms.foreach(x => result(x._1) = x._2)
    result.toSeq
  }

}

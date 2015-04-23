package pl.edu.pw.elka.cnv.coverage

import htsjdk.samtools.SAMRecord
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.cnv.utils.Convertions

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class CoverageCounter(@transient sc: SparkContext, bedFile: RDD[(Int, (String, Int, Int))], reads: RDD[(Int, SAMRecord)],
                      parseCigar: Boolean = false, countingMode: Int = CountingMode.COUNT_WHER_STARTS, reduceWorkers: Int = 12)
  extends Serializable with Convertions {

  private val featuresMap: Broadcast[mutable.HashMap[String, Array[ArrayBuffer[(Int, Int, Int)]]]] = sc.broadcast {
    bedFileToFeaturesMap(bedFile)
  }

  def genCoverage: RDD[(Long, Int)] =
    reads.mapPartitions(partition => {
      val featuresCountMap = new mutable.HashMap[Long, Int]

      for ((sampleId, read) <- partition)
        if (featuresMap.value.contains(read.getReferenceName)) {
          val features = featuresMap.value(read.getReferenceName)
          val bases = genBases(read)
          for ((baseStart, baseEnd) <- bases) {
            val featuresToCheck = getFeaturesToCheck(baseStart, features)
            if (featuresToCheck != null)
              for ((featureId, featureStart, featureEnd) <- featuresToCheck)
                if (countingCondition(baseStart, baseEnd, featureStart, featureEnd)) {
                  val coverageId = encodeCoverageId(sampleId, featureId)
                  if (!featuresCountMap.contains(coverageId))
                    featuresCountMap(coverageId) = 1
                  else
                    featuresCountMap(coverageId) += 1
                }
          }
        }

      featuresCountMap.iterator
    }).reduceByKey(_ + _, reduceWorkers)

  private def countingCondition(baseStart: Int, baseEnd: Int, featureStart: Int, featureEnd: Int): Boolean =
    countingMode match {
      case CountingMode.COUNT_WHEN_WHITIN =>
        if (baseStart >= featureStart && baseEnd <= featureEnd) true
        else false
      case CountingMode.COUNT_WHEN_OVERLAPS =>
        if ((baseStart >= featureStart && baseStart <= featureEnd)
          || (baseStart <= featureStart && baseEnd >= featureEnd)
          || (baseEnd >= featureStart && baseEnd <= featureEnd)) true
        else false
      case CountingMode.COUNT_WHER_STARTS =>
        if (baseStart >= featureStart && baseStart <= featureEnd) true
        else false
    }

  private def getFeaturesToCheck(baseStart: Int, features: Array[ArrayBuffer[(Int, Int, Int)]]): ArrayBuffer[(Int, Int, Int)] = {
    val startId = baseStart / 10000
    var result = features(startId)

    if (startId > 0 && result != null && features(startId - 1) != null)
      result = result ++ (features(startId - 1))
    else if (startId > 0 && result == null && features(startId - 1) != null)
      result = features(startId - 1)

    result
  }

  private def genBases(read: SAMRecord): ArrayBuffer[(Int, Int)] =
    if (parseCigar) genBasesFromCigar(read.getAlignmentStart, read.getCigar)
    else ArrayBuffer((read.getAlignmentStart, read.getAlignmentEnd))

  private def genBasesFromCigar(alignStart: Int, cigar: htsjdk.samtools.Cigar): ArrayBuffer[(Int, Int)] = {
    val result = new ArrayBuffer[(Int, Int)]
    val numCigElem = cigar.numCigarElements

    var shift = 0
    for (i <- 0 until numCigElem) {
      val cigElem = cigar.getCigarElement(i)
      if ((i == 0 && cigElem.getOperator.toString == "M") || (i == 1 && cigar.getCigarElement(0).getOperator.toString == "S"))
        result += ((alignStart, alignStart + cigElem.getLength))
      else if (cigElem.getOperator.toString != "M")
        shift += cigElem.getLength
      else if (i > 1 && result.length > 0 && cigElem.getOperator.toString == "M") {
        val temp = result.last._2 + shift
        result += ((temp, temp + cigElem.getLength))
        shift = 0
      }
    }

    result
  }

}

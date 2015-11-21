package pl.edu.pw.elka.cnv.rpkm

import org.scalatest.Matchers
import pl.edu.pw.elka.cnv.SparkFunSuite
import pl.edu.pw.elka.cnv.utils.FileUtils

/**
 * Created by mariusz-macbook on 10/07/15.
 */
class RpkmsCounterFunSuite extends SparkFunSuite with Matchers {

  val files = new FileUtils with Serializable

  sparkTest("calculateRpkms test") {
    val samples = files.scanForSamples(getClass.getResource("/").getPath)

    val reads1 = files.loadReads(sc, samples).map(r => (0, r._2))
    val reads2 = files.loadReads(sc, samples).map(r => (1, r._2))
    val reads3 = files.loadReads(sc, samples).map(r => (2, r._2))

    val bedFile = sc.broadcast {
      files.readBedFile(getClass.getResource("/test_bed_file.txt").getPath)
    }

    val coverage = sc.objectFile[(Int, Iterable[(Int, Int)])](getClass.getResource("/coverage.txt").getPath)
    val counter = new RpkmsCounter(reads1.union(reads2).union(reads3), bedFile, coverage)
    val rpkms = counter.calculateRpkms.collectAsMap

    rpkms.keys should have size (220544)

    rpkms(41949) should contain theSameElementsAs Array(2338.0642765043804, 8652.428342982195, 3658.195806775561)
    rpkms(145042) should contain theSameElementsAs Array(1466.4096587948284, 2179.3602111370656, 1709.4609834569546)
    rpkms(199943) should contain theSameElementsAs Array(4814.731002830691, 9316.077147488972, 5213.584458686489)
    rpkms(10757) should contain theSameElementsAs Array(5442.114717722091, 4235.2391745722225, 6079.0768099400775)
    rpkms(165304) should contain theSameElementsAs Array(2609.4922177232206, 4507.304739703744, 4015.906854548073)
    rpkms(34990) should contain theSameElementsAs Array(5112.57611596976, 6635.471129237349, 7342.529528254443)
    rpkms(26655) should contain theSameElementsAs Array(13171.18334249432, 7647.363622199966, 10435.329235744757)
    rpkms(19289) should contain theSameElementsAs Array(11581.782385239812, 11816.284165818346, 16024.288339533148)
  }

}
package pl.edu.pw.elka.cnv.zrpkm

import org.scalatest.Matchers
import pl.edu.pw.elka.cnv.SparkFunSuite
import pl.edu.pw.elka.cnv.coverage.CoverageCounter
import pl.edu.pw.elka.cnv.utils.FileUtils

/**
 * Created by mariusz-macbook on 10/07/15.
 */
class ZrpkmsCounterFunSuite extends SparkFunSuite with Matchers {

  sparkTest("calculateZrpkms test") {
    val rpkms = sc.objectFile[(Int, Array[Double])](getClass.getResource("/rpkms.txt").toString)
    val counter = new ZrpkmsCounter(rpkms, 1.0)
    val zrpkms = counter.calculateZrpkms.collectAsMap

    zrpkms.keys should have size (205009)

    zrpkms(41949) should contain theSameElementsAs Array(-0.6187888184759598, 1.743122411301341, 0.0)
    zrpkms(145042) should contain theSameElementsAs Array(0.6753340548098626, -1.7014364363287144, 0.0)
    zrpkms(199943) should contain theSameElementsAs Array(0.0, 0.9048617777046134, -1.5188425250700075)
    zrpkms(10757) should contain theSameElementsAs Array(0.3609382478923746, -1.917694998782606, 0.0)
    zrpkms(165304) should contain theSameElementsAs Array(0.0, -0.3392699479290128, 1.9312391124157755)
    zrpkms(34990) should contain theSameElementsAs Array(0.0, -1.5137962536756966, 0.9108295717892599)
    zrpkms(26655) should contain theSameElementsAs Array(1.2795314545366674, -1.1691288088056908, 0.0)
    zrpkms(19289) should contain theSameElementsAs  Array(0.0, -1.808370725734326, 0.526669797074028)
  }

}
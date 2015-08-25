package pl.edu.pw.elka.cnv.utils

import org.scalatest.Matchers
import pl.edu.pw.elka.cnv.SparkFunSuite

/**
 * Created by mariusz-macbook on 25/08/15.
 */
class CNVUtilsFunSuite extends SparkFunSuite with Matchers {

  val cnv = new CNVUtils with Serializable

  test("rpkm test") {
    cnv.rpkm(8, 2845, 847395L) should be(3.318347159071224)
    cnv.rpkm(59, 52917, 1539749L) should be(7.241138695926769E-1)
    cnv.rpkm(123, 927492, 26465253L) should be(5.0109363817965906E-3)
  }

  test("zrpkm test") {
    cnv.zrpkm(3.318347159071224, 1.0, 345.7439018439) should be(6.705388429722572E-3)
    cnv.zrpkm(0.7241138695926769, 1.5, 1010.1908008888887) should be(-7.680589941272522E-4)
    cnv.zrpkm(0.0050109363817965906, 2.0, 38438.29120355556) should be(-5.190108616050201E-5)
  }

}

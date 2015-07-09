package pl.edu.pw.elka.cnv.utils

import org.scalatest.Matchers
import pl.edu.pw.elka.cnv.SparkFunSuite

/**
 * Created by mariusz-macbook on 09/07/15.
 */
class StatUtilsFunSuite extends SparkFunSuite with Matchers {

  val stats = new StatUtils with Serializable

  test("stddev test") {
    stats.stddev(Seq(0.0, 0.0, 0.0)) should be(0.0)
    stats.stddev(Seq(5.523, 83.273, 40.925)) should be(1010.1908008888887)
    stats.stddev(Seq(264.3, 734.96, 582.284)) should be(38438.29120355556)
  }

  test("median test") {
    stats.median(Seq(1.0)) should be(1.0)
    stats.median(Seq(2.0, 1.0)) should be(1.5)
    stats.median(Seq(2.0, 1.0, 3.0)) should be(2.0)
  }

  test("rpkm test") {
    stats.rpkm(8, 2845, 847395L) should be(3.318347159071224)
    stats.rpkm(59, 52917, 1539749L) should be(7.241138695926769E-1)
    stats.rpkm(123, 927492, 26465253L) should be(5.0109363817965906E-3)
  }

  test("zrpkm test") {
    stats.zrpkm(3.318347159071224, 1.0, 345.7439018439) should be(6.705388429722572E-3)
    stats.zrpkm(0.7241138695926769, 1.5, 1010.1908008888887) should be(-7.680589941272522E-4)
    stats.zrpkm(0.0050109363817965906, 2.0, 38438.29120355556) should be(-5.190108616050201E-5)
  }

}

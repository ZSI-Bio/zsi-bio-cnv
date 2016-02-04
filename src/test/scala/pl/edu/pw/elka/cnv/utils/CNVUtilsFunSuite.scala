package pl.edu.pw.elka.cnv.utils

import org.scalatest.Matchers
import pl.edu.pw.elka.cnv.SparkFunSuite
import pl.edu.pw.elka.cnv.utils.CNVUtils.{blackman, convolve, rpkm, zrpkm}

/**
 * Created by mariusz-macbook on 25/08/15.
 */
class CNVUtilsFunSuite extends SparkFunSuite with Matchers {

  test("rpkm test") {
    rpkm(8, 2845, 847395L) should be(3.318347159071224)
    rpkm(59, 52917, 1539749L) should be(7.241138695926769E-1)
    rpkm(123, 927492, 26465253L) should be(5.0109363817965906E-3)
  }

  test("zrpkm test") {
    zrpkm(3.318347159071224, 1.0, 345.7439018439) should be(6.705388429722572E-3)
    zrpkm(0.7241138695926769, 1.5, 1010.1908008888887) should be(-7.680589941272522E-4)
    zrpkm(0.0050109363817965906, 2.0, 38438.29120355556) should be(-5.190108616050201E-5)
  }

  test("blackman test") {
    val b3 = blackman(3)
    b3 should have size (3)
    b3 should contain theSameElementsAs
      Array(-1.3877787807814457E-17, 0.9999999999999999, -1.3877787807814457E-17)

    val b5 = blackman(5)
    b5 should have size (5)
    b5 should contain theSameElementsAs
      Array(-1.3877787807814457E-17, 0.3399999999999999, 0.9999999999999999, 0.3400000000000001, -1.3877787807814457E-17)

    val b7 = blackman(7)
    b7 should have size (7)
    b7 should contain theSameElementsAs
      Array(-1.3877787807814457E-17, 0.12999999999999995, 0.6299999999999999, 0.9999999999999999, 0.6300000000000002, 0.13000000000000023, -1.3877787807814457E-17)
  }

  test("convolve test") {
    val c1 = convolve(Array(1, 2, 3), Array(4, 5, 6))
    c1 should have size (5)
    c1 should contain theSameElementsAs
      Array(4.0, 13.0, 28.0, 27.0, 18.0)

    val c2 = convolve(Array(5.3, 8.4, 1.2), Array(9.3, 7.4, 1.9, 2.5))
    c2 should have size (6)
    c2 should contain theSameElementsAs
      Array(49.29, 117.34, 83.39, 38.09, 23.28, 3.0)


    val c3 = convolve(Array(0.4, 2.5, 7.3, 2.6, 3.4), Array(7.2, 3.5, 9.4, 0.2))
    c3 should have size (8)
    c3 should contain theSameElementsAs
      Array(2.8800000000000003, 19.4, 65.07000000000001, 67.85, 102.7, 37.800000000000004, 32.480000000000004, 0.68)
  }

}

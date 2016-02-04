package pl.edu.pw.elka.cnv.utils

import org.scalatest.Matchers
import pl.edu.pw.elka.cnv.SparkFunSuite
import pl.edu.pw.elka.cnv.utils.StatUtils.{mean, median, stddev}

/**
 * Created by mariusz-macbook on 09/07/15.
 */
class StatUtilsFunSuite extends SparkFunSuite with Matchers {

  test("mean test") {
    mean(Array(0.0, 0.0, 0.0)) should be(0.0)
    mean(Array(3.0, 2.0, 1.0)) should be(2.0)
    mean(Array(2.1, 1.4, 3.7)) should be(2.4)
  }

  test("stddev test") {
    stddev(Array(0.0, 0.0, 0.0)) should be(0.0)
    stddev(Array(5.523, 83.273, 40.925)) should be(31.78349887738744)
    stddev(Array(264.3, 734.96, 582.284)) should be(196.05685706844216)
  }

  test("median test") {
    median(Array(1.0)) should be(1.0)
    median(Array(2.0, 1.0)) should be(1.5)
    median(Array(2.0, 1.0, 3.0)) should be(2.0)
  }

}

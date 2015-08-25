package pl.edu.pw.elka.cnv.utils

import org.scalatest.Matchers
import pl.edu.pw.elka.cnv.SparkFunSuite

/**
 * Created by mariusz-macbook on 09/07/15.
 */
class StatUtilsFunSuite extends SparkFunSuite with Matchers {

  val stats = new StatUtils with Serializable

  test("mean test") {
    stats.mean(Seq(0.0, 0.0, 0.0)) should be(0.0)
    stats.mean(Seq(3.0, 2.0, 1.0)) should be(2.0)
    stats.mean(Seq(2.1, 1.4, 3.7)) should be(2.4)
  }

  test("stddev test") {
    stats.stddev(Seq(0.0, 0.0, 0.0)) should be(0.0)
    stats.stddev(Seq(5.523, 83.273, 40.925)) should be(31.78349887738744)
    stats.stddev(Seq(264.3, 734.96, 582.284)) should be(196.05685706844216)
  }

  test("median test") {
    stats.median(Seq(1.0)) should be(1.0)
    stats.median(Seq(2.0, 1.0)) should be(1.5)
    stats.median(Seq(2.0, 1.0, 3.0)) should be(2.0)
  }

}

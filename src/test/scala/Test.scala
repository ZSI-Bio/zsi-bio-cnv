import org.scalatest.Matchers

/**
 * Created by mariusz-macbook on 27/04/15.
 */
class Test extends SparkFunSuite with Matchers {

  sparkTest("spark code") {
    sc.parallelize(1 to 10).filter {
      _ % 2 == 0
    }.count should be(5)
  }

  test("non-spark code") {
    (12 + 8) should be(20)
  }

}

package pl.edu.pw.elka.cnv

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
 * Created by mariusz-macbook on 27/04/15.
 */
trait SparkFunSuite extends FunSuite {

  var sc: SparkContext = _

  def sparkTest(name: String)(body: => Unit): Unit =
    test(name, SparkTest) {
      sc = createSpark(name)
      try {
        body
      } finally {
        destroySpark()
      }
    }

  private def createSpark(appName: String): SparkContext = {
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[4]")
      .set("spark.driver.host", "127.0.0.1")
    new SparkContext(conf)
  }

  private def destroySpark(): Unit = {
    sc.stop()
    sc = null
  }

}

package pl.edu.pw.elka.cnv.coverage

import org.scalatest.Matchers
import pl.edu.pw.elka.cnv.SparkFunSuite
import pl.edu.pw.elka.cnv.utils.FileUtils

/**
 * Created by mariusz-macbook on 10/07/15.
 */
class CoverageCounterFunSuite extends SparkFunSuite with Matchers {

  val files = new FileUtils with Serializable

  sparkTest("calculateReadCoverage test") {
    val samples = files.scanForSamples(getClass.getResource("/").toString)
    val reads = files.loadReads(sc, samples)
    val bedFile = sc.broadcast {
      files.readRegionFile(sc, getClass.getResource("/test_bed_file.bed").toString)
    }

    val counter = new CoverageCounter(sc, bedFile, reads, Array.empty, false, CountingMode.COUNT_WHEN_STARTS)
    val coverage = counter.calculateReadCoverage.collectAsMap

    coverage.keys should have size (80997)

    coverage(41949) should be(1)
    coverage(145042) should be(2)
    coverage(199943) should be(3)
    coverage(10757) should be(4)
    coverage(165304) should be(5)
    coverage(34990) should be(6)
    coverage(26655) should be(7)
    coverage(19289) should be(8)
  }

}
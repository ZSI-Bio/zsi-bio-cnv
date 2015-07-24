package pl.edu.pw.elka.cnv.utils

import java.io.Serializable

import org.scalatest.Matchers
import pl.edu.pw.elka.cnv.SparkFunSuite

/**
 * Created by mariusz-macbook on 30/04/15.
 */
class ConversionUtilsFunSuite extends SparkFunSuite with Matchers {

  val convertions = new ConvertionUtils with Serializable

  sparkTest("bedFileToChromosomesMap test") {
    val input = sc parallelize {
      Array((2429, (1, 19203909, 19204106)),
        (101874, (10, 113928069, 113928282)),
        (179177, (20, 47115835, 47116753)))
    }
    val output = convertions.bedFileToChromosomesMap(input)

    output.keys should have size (3)
    all(output.values) should have size (25000)

    output.keys should contain theSameElementsAs Array(1, 10, 20)
    output(1)(19203909 / 10000) should contain theSameElementsAs Array((2429, 19203909, 19204106))
    output(10)(113928069 / 10000) should contain theSameElementsAs Array((101874, 113928069, 113928282))
    output(20)(47115835 / 10000) should contain theSameElementsAs Array((179177, 47115835, 47116753))
  }

  sparkTest("coverageToRegionCoverage test") {
    val input = sc parallelize {
      Array((0L, 100),
        (5000425385L, 200),
        (12092619574L, 300))
    }
    val output = convertions.coverageToRegionCoverage(input).collect.toMap

    output.keys should have size (3)
    all(output.values) should have size (1)

    output.keys should contain theSameElementsAs Array(0, 425385, 92619574)
    output(0) should contain theSameElementsAs Array((0, 100))
    output(425385) should contain theSameElementsAs Array((5, 200))
    output(92619574) should contain theSameElementsAs Array((12, 300))
  }

  test("encodeCoverageId test") {
    convertions.encodeCoverageId(0, 0) should be(0L)
    convertions.encodeCoverageId(5, 425385) should be(5000425385L)
    convertions.encodeCoverageId(12, 92619574) should be(12092619574L)
  }

  test("decodeCoverageId test") {
    convertions.decodeCoverageId(0L) should be((0, 0))
    convertions.decodeCoverageId(5000425385L) should be((5, 425385))
    convertions.decodeCoverageId(12092619574L) should be((12, 92619574))
  }

  test("chrStrToInt test") {
    convertions.chrStrToInt("chr10") should be(10)
    convertions.chrStrToInt("chrX") should be(23)
    convertions.chrStrToInt("test") should be(0)
  }

}

package pl.edu.pw.elka.cnv.caller

import org.apache.commons.math3.linear.RealMatrix
import org.scalatest.Matchers
import pl.edu.pw.elka.cnv.SparkFunSuite
import pl.edu.pw.elka.cnv.utils.FileUtils.readRegionFile

/**
 * Created by mariusz-macbook on 06/10/15.
 */
class CallerFunSuite extends SparkFunSuite with Matchers {

  sparkTest("caller call test") {
    val bedFile = sc.broadcast {
      readRegionFile(sc, getClass.getResource("/test_bed_file.bed").toString)
    }

    val matrices = sc.objectFile[(Int, Array[Int], RealMatrix)](getClass.getResource("/matrices.txt").toString)
    val caller = new Caller(bedFile, matrices, 1.5)
    val calls = caller.call.sortBy(_._3).collect

    calls should have size (168)

    calls(150) should be((0, 5, 140890514, 140890740, "del"))
    calls(19) should be((0, 10, 18787284, 18787406, "dup"))
    calls(22) should be((0, 14, 20344427, 20404425, "dup"))
    calls(49) should be((0, 20, 43047065, 43047152, "del"))
    calls(157) should be((0, 1, 161202597, 161202736, "dup"))
    calls(26) should be((0, 6, 26406137, 26408180, "dup"))
    calls(28) should be((0, 21, 27277335, 27277389, "del"))
    calls(120) should be((0, 9, 98247967, 98660253, "del"))
    calls(35) should be((0, 13, 33703466, 33703665, "dup"))
    calls(96) should be((0, 2, 71762397, 71762441, "dup"))
    calls(41) should be((0, 17, 37922443, 37922746, "dup"))
    calls(38) should be((0, 22, 35730321, 35743202, "del"))
  }

}

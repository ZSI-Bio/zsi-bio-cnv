package pl.edu.pw.elka.cnv.caller

import org.apache.commons.math3.linear.RealMatrix
import org.scalatest.Matchers
import pl.edu.pw.elka.cnv.SparkFunSuite

/**
 * Created by mariusz-macbook on 06/10/15.
 */
class ChrCallerFunSuite extends SparkFunSuite with Matchers {

  sparkTest("chr caller call test") {
    val matrix = sc.objectFile[(Int, Array[Int], RealMatrix)](getClass.getResource("/matrices.txt").toString)
      .sortBy(_._1).collect.apply(9)
    val caller = new ChrCaller(matrix._2, matrix._3, 1.5)
    val calls = caller.call

    calls should have size (9)

    calls(0) should be((0, 25251, 25256, "dup"))
    calls(1) should be((0, 26321, 26326, "dup"))
    calls(2) should be((0, 31448, 31451, "dup"))
    calls(3) should be((0, 31671, 31674, "dup"))
    calls(4) should be((0, 29271, 29275, "del"))
    calls(5) should be((0, 31411, 31421, "del"))
    calls(6) should be((0, 33582, 33584, "del"))
    calls(7) should be((0, 34093, 34096, "del"))
    calls(8) should be((2, 30229, 30234, "dup"))
  }

}

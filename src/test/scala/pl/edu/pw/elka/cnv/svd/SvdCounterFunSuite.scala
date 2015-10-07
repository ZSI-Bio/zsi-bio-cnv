package pl.edu.pw.elka.cnv.svd

import org.scalatest.Matchers
import pl.edu.pw.elka.cnv.SparkFunSuite
import pl.edu.pw.elka.cnv.utils.FileUtils

/**
 * Created by mariusz-macbook on 25/08/15.
 */
class SvdCounterFunSuite extends SparkFunSuite with Matchers {

  val files = new FileUtils with Serializable

  sparkTest("calculateSvd test") {
    val bedFile = sc.broadcast {
      files.readBedFile("resources/data/test_bed_file.txt")
    }

    val zrpkms = sc.objectFile[(Int, Array[Double])]("resources/data/zrpkms.txt")
    val counter = new SvdCounter(bedFile, zrpkms, 1)
    val matrices = counter.calculateSvd.collect

    matrices should have size (24)

    matrices(0)._1 should be(24)
    matrices(0)._2 should have size (244)
    matrices(0)._3.getColumnDimension should be(3)
    matrices(0)._3.getRowDimension should be(244)

    matrices(5)._1 should be(2)
    matrices(5)._2 should have size (14698)
    matrices(5)._3.getColumnDimension should be(3)
    matrices(5)._3.getRowDimension should be(14698)

    matrices(11)._1 should be(5)
    matrices(11)._2 should have size (9320)
    matrices(11)._3.getColumnDimension should be(3)
    matrices(11)._3.getRowDimension should be(9320)

    matrices(17)._1 should be(20)
    matrices(17)._2 should have size (5895)
    matrices(17)._3.getColumnDimension should be(3)
    matrices(17)._3.getRowDimension should be(5895)

    matrices(23)._1 should be(23)
    matrices(23)._2 should have size (8288)
    matrices(23)._3.getColumnDimension should be(3)
    matrices(23)._3.getRowDimension should be(8288)
  }

}

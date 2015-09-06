package pl.edu.pw.elka.cnv.caller

import org.apache.commons.math3.linear.{BlockRealMatrix, RealMatrix}
import pl.edu.pw.elka.cnv.utils.{CNVUtils, StatUtils}

/**
 * Created by mariusz-macbook on 06/09/15.
 */
class ChrCaller(regions: Array[Int], matrix: RealMatrix, threshold: Double) extends Serializable with CNVUtils with StatUtils {

  private val smoothed: RealMatrix = smooth(matrix, 15)
  private val means: Array[Double] = smoothed.getData.map(mean)
  private val stddevs: Array[Double] = smoothed.getData.map(stddev)

  def call: Array[(Int, Int, Int, String)] =
    for {
      idx <- (0 until smoothed.getColumnDimension).toArray
      column = smoothed.getColumn(idx)

      dupBreakPoints = getBreakPoints(column, threshold)
      dupCalls = getCalls(dupBreakPoints, column, "dup")
      mergedDupCalls = mergeCalls(dupCalls)

      delBreakPoints = getBreakPoints(column, -threshold)
      delCalls = getCalls(delBreakPoints, column, "del")
      mergedDelCalls = mergeCalls(delCalls)

      (start, stop, state) <- mergedDupCalls ++ mergedDelCalls
    } yield (idx, regions(start), regions(stop), state)

  private def smooth(matrix: RealMatrix, window: Int): RealMatrix =
    if (window <= 0) matrix
    else {
      val blackmanWindow = blackman(window)
      val blackmanWindowSum = blackmanWindow.sum
      val weightings = blackmanWindow.map(_ / blackmanWindowSum)
      val rows = matrix.transpose.getData map {
        case column =>
          convolve(column, weightings).drop((window - 1) / 2)
            .take(matrix.getRowDimension)
      }
      new BlockRealMatrix(rows).transpose
    }

  private def getBreakPoints(data: Array[Double], threshold: Double): Array[(Int, Int)] = {

    def cond(elem: Double): Boolean =
      if (threshold >= 0) elem >= threshold
      else elem <= threshold

    val result = (0 to data.size) filter {
      case idx if idx == 0 => cond(data(idx))
      case idx if idx == data.size => cond(data(idx - 1))
      case idx => cond(data(idx - 1)) ^ cond(data(idx))
    } toArray

    (0 until result.size by 2) map {
      idx => (result(idx), result(idx + 1))
    } toArray
  }

  private def getCalls(breakPoints: Array[(Int, Int)], column: Array[Double], state: String): Array[(Int, Int, String)] = {

    def cond(idx: Int): Boolean =
      if (state == "dup") column(idx) < means(idx) + 3 * stddevs(idx)
      else column(idx) > -means(idx) - 3 * stddevs(idx)

    def chooseStart(starts: Seq[Int]): Int =
      if (starts.isEmpty) 0
      else starts.max

    def chooseStop(stop: Int, stops: Seq[Int]): Int =
      if (stops.isEmpty) column.size - 1
      else stops.min

    breakPoints map {
      case (start, stop) =>
        val starts = (0 until start).filter(cond)
        val stops = (stop until column.size).filter(cond)
        (chooseStart(starts), chooseStop(stop, stops), state)
    }
  }

  private def mergeCalls(calls: Array[(Int, Int, String)]): List[(Int, Int, String)] = {

    def merge(pStart: Int, pStop: Int, pCalls: List[(Int, Int, String)]): List[(Int, Int, String)] =
      pCalls match {
        case Nil => List((pStart, pStop, calls.head._3))
        case (start, stop, state) :: xs =>
          if (start <= pStop) merge(pStart, math.max(stop, pStop), xs)
          else (pStart, pStop, state) :: merge(start, stop, xs)
      }

    calls.sorted.toList match {
      case Nil => List()
      case x :: xs => merge(x._1, x._2, x :: xs)
    }
  }

}

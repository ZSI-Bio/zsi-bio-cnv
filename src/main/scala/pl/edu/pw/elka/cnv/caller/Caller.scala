package pl.edu.pw.elka.cnv.caller

import org.apache.commons.math3.linear.RealMatrix
import pl.edu.pw.elka.cnv.utils.{CNVUtils, StatUtils}

/**
 * Created by mariusz-macbook on 30/08/15.
 */
class Caller(matrix: RealMatrix, threshold: Double) extends Serializable with CNVUtils with StatUtils {

  private val smoothed: RealMatrix = smooth(matrix, 15)
  private val means = smoothed.getData.map(mean)
  private val stddevs = smoothed.getData.map(stddev)

  def call: Array[(Int, Int, Int, String)] =
    for {
      idx <- (0 until matrix.getColumnDimension).toArray
      column = smoothed.getColumn(idx)

      dupBreakPoints = getBreakPoints(column, threshold)
      dupCalls = getCalls(dupBreakPoints, column, "dup")
      mergedDupCalls = mergeCalls(dupCalls)

      delBreakPoints = getBreakPoints(column, -threshold)
      delCalls = getCalls(delBreakPoints, column, "del")
      mergedDelCalls = mergeCalls(delCalls)

      (start, stop, state) <- mergedDupCalls ++ mergedDelCalls
    } yield (idx, start, stop, state)

  private def smooth(matrix: RealMatrix, window: Int): RealMatrix = {
    if (window > 0) {
      val blackmanWindow = blackman(window)
      val blackmanWindowSum = blackmanWindow.sum
      val weightings = blackmanWindow.map(_ / blackmanWindowSum)
      (0 until matrix.getColumnDimension) foreach {
        idx =>
          val convolved = convolve(matrix.getColumn(idx), weightings)
          val newColumn = convolved.drop((window - 1) / 2).take(matrix.getRowDimension)
          matrix.setColumn(idx, newColumn)
      }
    }
    matrix
  }

  private def getBreakPoints(data: Array[Double], threshold: Double): Array[(Int, Int)] = {

    def cond(elem: Double): Boolean =
      if (threshold >= 0) elem >= threshold
      else elem <= threshold

    val result = (1 until data.size) filter {
      idx => cond(data(idx - 1)) ^ cond(data(idx))
    } toArray

    if (cond(data.head))
      0 +: result
    if (cond(data.last))
      result :+ data.size

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
      else stop + stops.min

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
        case Nil => List()
        case (start, stop, state) :: xs =>
          if (start <= pStop) merge(pStart, math.max(stop, pStop), xs)
          else (pStart, pStop, state) :: merge(start, stop, xs)
      }

    calls.sortBy(_._1).toList match {
      case Nil => List()
      case x :: xs => merge(x._1, x._2, x :: xs)
    }
  }

}

package pl.edu.pw.elka.cnv.coverage

/**
 * Created by mariusz-macbook on 26/04/15.
 *
 * Object with values representing different approaches to calculation of coverage.
 */
object CountingMode {

  /**
   * Calculate coverage only based on all of the reads that are entirely within a single region.
   */
  val COUNT_WHEN_WHITIN = 1

  /**
   * Calculate coverage based on all of the reads that overlap a given region.
   */
  val COUNT_WHEN_OVERLAPS = 2

  /**
   * Calculate coverage based on reads that start within a given region.
   */
  val COUNT_WHEN_STARTS = 3

}

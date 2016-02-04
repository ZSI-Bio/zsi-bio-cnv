package pl.edu.pw.elka.cnv.coverage

/**
 * Object with values representing different approaches to calculation of coverage.
 */
object CountingMode {

  /**
   * Calculate coverage only based on all of the reads that are entirely within a single region.
   *
   * Example coverage result for this mode:
   * <img src="../../../../../../images/count_when_within.png" />
   */
  val COUNT_WHEN_WHITIN = 1

  /**
   * Calculate coverage based on all of the reads that overlap a given region.
   *
   * Example coverage result for this mode:
   * <img src="../../../../../../images/count_when_overlaps.png" />
   */
  val COUNT_WHEN_OVERLAPS = 2

  /**
   * Calculate coverage based on reads that start within a given region.
   *
   * Example coverage result for this mode:
   * <img src="../../../../../../images/count_when_starts.png" />
   */
  val COUNT_WHEN_STARTS = 3

}

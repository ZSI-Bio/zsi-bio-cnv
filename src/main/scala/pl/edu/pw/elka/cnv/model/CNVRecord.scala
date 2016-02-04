package pl.edu.pw.elka.cnv.model

import htsjdk.samtools.{AlignmentBlock, Cigar}

/**
 * Base trait for BAM and ADAM reads.
 * It implements adapter design pattern that works as a bridge
 * between two incompatible interfaces: SAMRecord and AlignmentRecord.
 */
trait CNVRecord extends Serializable {

  /**
   * Method that returns reference name of a given read.
   * This name in mapped from string to integer.
   *
   * @return Reference name.
   */
  def getReferenceName: Int

  /**
   * Method that returns alignment start of a given read.
   *
   * @return Alignment start.
   */
  def getAlignmentStart: Int

  /**
   * Method that returns alignment end of a given read.
   *
   * @return Alignment end.
   */
  def getAlignmentEnd: Int

  /**
   * Method that returns mapping quality of a given read.
   *
   * @return Mapping quality.
   */
  def getMappingQuality: Int

  /**
   * Method that returns length of a given read.
   *
   * @return Length.
   */
  def getReadLength: Int

  /**
   * Method that returns cigar string of a given read.
   *
   * @return Cigar string.
   */
  def getCigar: Cigar

  /**
   * Method that returns base qualities of a given read.
   *
   * @return Array containing base qualities.
   */
  def getBaseQualities: Array[Byte]

  /**
   * Method that returns alignment blocks of a given read.
   *
   * @return Array containing alignment blocks.
   */
  def getAlignmentBlocks: Array[AlignmentBlock]

  /**
   * Method that returns primary alignment flag of a given read.
   *
   * @return Primary alignment flag.
   */
  def getNotPrimaryAlignmentFlag: Boolean

  /**
   * Method that returns fails vendor quality check flag of a given read.
   *
   * @return Fails vendor quality check flag.
   */
  def getReadFailsVendorQualityCheckFlag: Boolean

  /**
   * Method that returns duplicate read flag of a given read.
   *
   * @return Duplicate read flag.
   */
  def getDuplicateReadFlag: Boolean

  /**
   * Method that returns unmapped flag of a given read.
   *
   * @return Unmapped flag.
   */
  def getReadUnmappedFlag: Boolean

}

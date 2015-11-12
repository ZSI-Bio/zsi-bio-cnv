package pl.edu.pw.elka.cnv.model

import htsjdk.samtools.{AlignmentBlock, Cigar}

/**
 * Created by mariusz-macbook on 07/11/15.
 */
trait CNVRecord extends Serializable {

  def getReferenceName: Int

  def getAlignmentStart: Int

  def getAlignmentEnd: Int

  def getMappingQuality: Int

  def getReadLength: Int

  def getCigar: Cigar

  def getBaseQualities: Array[Byte]

  def getAlignmentBlocks: Array[AlignmentBlock]

  def getNotPrimaryAlignmentFlag: Boolean

  def getReadFailsVendorQualityCheckFlag: Boolean

  def getDuplicateReadFlag: Boolean

  def getReadUnmappedFlag: Boolean

}

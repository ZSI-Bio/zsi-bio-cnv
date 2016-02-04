package pl.edu.pw.elka.cnv.model

import htsjdk.samtools.{AlignmentBlock, Cigar, SAMRecord}
import pl.edu.pw.elka.cnv.utils.ConversionUtils.chrStrToInt

import scala.collection.JavaConverters._

/**
 * Class that adapts SAMRecord to interface required by this application.
 * It implements adapter design pattern that works as a bridge
 * between two incompatible interfaces: SAMRecord and AlignmentRecord.
 */
class SAMRecordAdapter(record: SAMRecord) extends CNVRecord {

  override def getReferenceName: Int =
    chrStrToInt(record.getReferenceName)

  override def getAlignmentStart: Int =
    record.getAlignmentStart

  override def getAlignmentEnd: Int =
    record.getAlignmentEnd

  override def getMappingQuality: Int =
    record.getMappingQuality

  override def getReadLength: Int =
    record.getReadLength

  override def getCigar: Cigar =
    record.getCigar

  override def getBaseQualities: Array[Byte] =
    record.getBaseQualities

  override def getAlignmentBlocks: Array[AlignmentBlock] =
    record.getAlignmentBlocks.asScala.toArray

  override def getNotPrimaryAlignmentFlag: Boolean =
    record.getNotPrimaryAlignmentFlag

  override def getReadFailsVendorQualityCheckFlag: Boolean =
    record.getReadFailsVendorQualityCheckFlag

  override def getDuplicateReadFlag: Boolean =
    record.getDuplicateReadFlag

  override def getReadUnmappedFlag: Boolean =
    record.getReadUnmappedFlag

}

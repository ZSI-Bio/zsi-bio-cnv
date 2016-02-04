package pl.edu.pw.elka.cnv.model

import htsjdk.samtools.{AlignmentBlock, Cigar, SAMUtils, TextCigarCodec}
import org.bdgenomics.formats.avro.AlignmentRecord
import pl.edu.pw.elka.cnv.utils.ConversionUtils.chrStrToInt

import scala.collection.JavaConverters._

/**
 * Class that adapts AlignmentRecord to interface required by this application.
 * It implements adapter design pattern that works as a bridge
 * between two incompatible interfaces: SAMRecord and AlignmentRecord.
 */
class AlignmentRecordAdapter(record: AlignmentRecord) extends CNVRecord {

  private lazy val cigar: Cigar =
    TextCigarCodec.decode(record.getCigar)

  private lazy val baseQualities: Array[Byte] = record.getQual match {
    case null => Array()
    case tmp => SAMUtils.fastqToPhred(tmp)
  }

  private lazy val alignmentBlocks: Array[AlignmentBlock] =
    SAMUtils.getAlignmentBlocks(getCigar, getAlignmentStart, "read cigar").asScala.toArray

  override def getReferenceName: Int =
    chrStrToInt(record.getContig.getContigName)

  override def getAlignmentStart: Int =
    record.getStart.toInt + 1

  override def getAlignmentEnd: Int =
    record.getEnd.toInt

  override def getMappingQuality: Int =
    record.getMapq

  override def getReadLength: Int = record.getSequence match {
    case "*" => 0
    case tmp => tmp.length
  }

  override def getCigar: Cigar =
    cigar

  override def getBaseQualities: Array[Byte] =
    baseQualities

  override def getAlignmentBlocks: Array[AlignmentBlock] =
    alignmentBlocks

  override def getNotPrimaryAlignmentFlag: Boolean =
    !record.getPrimaryAlignment

  override def getReadFailsVendorQualityCheckFlag: Boolean =
    record.getFailedVendorQualityChecks

  override def getDuplicateReadFlag: Boolean =
    record.getDuplicateRead

  override def getReadUnmappedFlag: Boolean =
    !record.getReadMapped

}
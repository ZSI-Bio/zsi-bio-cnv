package pl.edu.pw.elka.cnv.model

import htsjdk.samtools.{AlignmentBlock, Cigar, SAMUtils, TextCigarCodec}
import org.bdgenomics.formats.avro.AlignmentRecord
import pl.edu.pw.elka.cnv.utils.ConvertionUtils

import scala.collection.JavaConverters._

/**
 * Created by mariusz-macbook on 07/11/15.
 */
class AlignmentRecordAdapter(record: AlignmentRecord) extends CNVRecord with ConvertionUtils {

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
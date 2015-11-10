package pl.edu.pw.elka.cnv.model

import java.util

import htsjdk.samtools.util.StringUtil
import htsjdk.samtools.{AlignmentBlock, Cigar, SAMUtils, TextCigarCodec}
import org.bdgenomics.formats.avro.AlignmentRecord
import pl.edu.pw.elka.cnv.utils.ConvertionUtils

/**
 * Created by mariusz-macbook on 07/11/15.
 */
class AlignmentRecordAdapter(record: AlignmentRecord) extends CNVRecord with ConvertionUtils {

  private lazy val cigar: Cigar =
    TextCigarCodec.decode(record.getCigar)

  private lazy val alignmentBlocks: util.List[AlignmentBlock] =
    SAMUtils.getAlignmentBlocks(getCigar, getAlignmentStart, "read cigar")

  private lazy val readBases: Array[Byte] = record.getSequence match {
    case "*" => Array()
    case tmp =>
      val bases = StringUtil.stringToBytes(tmp)
      normalizeBases(bases)
  }

  private lazy val baseQualities: Array[Byte] = record.getQual match {
    case null => Array()
    case tmp => SAMUtils.fastqToPhred(tmp)
  }

  override def getReferenceName: Int =
    chrStrToInt(record.getContig.getContigName)

  override def getAlignmentStart: Int =
    record.getStart.toInt + 1

  override def getAlignmentEnd: Int =
    record.getEnd.toInt

  override def getCigar: Cigar =
    cigar

  override def getReadLength: Int =
    readBases.length

  override def getBaseQualitiesLength: Int =
    baseQualities.length

  override def getAlignmentBlocksLength: Int =
    alignmentBlocks.size

  override def getNotPrimaryAlignmentFlag: Boolean =
    !record.getPrimaryAlignment

  override def getReadFailsVendorQualityCheckFlag: Boolean =
    record.getFailedVendorQualityChecks

  override def getDuplicateReadFlag: Boolean =
    record.getDuplicateRead

  override def getReadUnmappedFlag: Boolean =
    !record.getReadMapped

  private def normalizeBases(bases: Array[Byte]): Array[Byte] =
    bases map {
      case base =>
        val tmp = StringUtil.toUpperCase(base)
        if (tmp == 46.toByte) 78.toByte else tmp
    }

}
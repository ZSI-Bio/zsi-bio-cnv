package pl.edu.pw.elka.cnv.model

import htsjdk.samtools.{Cigar, TextCigarCodec}
import org.bdgenomics.formats.avro.AlignmentRecord
import pl.edu.pw.elka.cnv.utils.ConvertionUtils

/**
 * Created by mariusz-macbook on 07/11/15.
 */
class AlignmentRecordAdapter(record: AlignmentRecord) extends CNVRecord with ConvertionUtils {

  override def getReferenceName: Int =
    chrStrToInt(record.getContig.getContigName)

  override def getAlignmentStart: Int =
    record.getStart.toInt + 1

  override def getAlignmentEnd: Int =
    record.getEnd.toInt

  override def getCigar: Cigar =
    TextCigarCodec.decode(record.getCigar)

  override def getNotPrimaryAlignmentFlag: Boolean =
    !record.getPrimaryAlignment

  override def getReadFailsVendorQualityCheckFlag: Boolean =
    record.getFailedVendorQualityChecks

  override def getDuplicateReadFlag: Boolean =
    record.getDuplicateRead

  override def getReadUnmappedFlag: Boolean =
    !record.getReadMapped

}
package pl.edu.pw.elka.cnv.model

import htsjdk.samtools.{Cigar, SAMRecord}
import pl.edu.pw.elka.cnv.utils.ConvertionUtils

/**
 * Created by mariusz-macbook on 07/11/15.
 */
class SAMRecordAdapter(record: SAMRecord) extends CNVRecord with ConvertionUtils {

  override def getReferenceName: Int =
    chrStrToInt(record.getReferenceName)

  override def getAlignmentStart: Int =
    record.getAlignmentStart

  override def getAlignmentEnd: Int =
    record.getAlignmentEnd

  override def getCigar: Cigar =
    record.getCigar

}

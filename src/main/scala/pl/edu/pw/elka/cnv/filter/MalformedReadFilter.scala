package pl.edu.pw.elka.cnv.filter

import htsjdk.samtools.CigarOperator
import pl.edu.pw.elka.cnv.model.CNVRecord

/**
 * Created by mariusz-macbook on 09/11/15.
 */
class MalformedReadFilter extends ReadFilter {

  override def filterOut(read: CNVRecord): Boolean =
    !checkInvalidAlignmentStart(read) ||
      !checkInvalidAlignmentEnd(read) ||
      !checkMismatchingBasesAndQuals(read) ||
      !checkCigarDisagreesWithAlignment(read) ||
      !checkSeqStored(read) ||
      !checkCigarIsSupported(read)

  private def checkInvalidAlignmentStart(read: CNVRecord): Boolean =
    if (!read.getReadUnmappedFlag &&
      (read.getAlignmentStart == 0 || read.getAlignmentStart == -1)) false
    else true

  private def checkInvalidAlignmentEnd(read: CNVRecord): Boolean =
    if (!read.getReadUnmappedFlag && read.getAlignmentEnd != -1 &&
      (read.getAlignmentEnd - read.getAlignmentStart + 1) < 0) false
    else true

  private def checkMismatchingBasesAndQuals(read: CNVRecord): Boolean =
    if (read.getReadLength == read.getBaseQualitiesLength) true
    else false

  private def checkCigarDisagreesWithAlignment(read: CNVRecord): Boolean =
    if (!read.getReadUnmappedFlag &&
      read.getAlignmentStart != -1 &&
      read.getAlignmentStart != 0 &&
      read.getAlignmentBlocksLength < 0) false
    else true

  private def checkSeqStored(read: CNVRecord): Boolean =
    if (read.getReadLength != 0) true
    else false

  private def checkCigarIsSupported(read: CNVRecord): Boolean = {
    val cigar = read.getCigar

    if (cigar == null)
      return true

    val result = (0 until cigar.getCigarElements.size) exists {
      idx => cigar.getCigarElement(idx) == CigarOperator.N
    }

    !result
  }

}

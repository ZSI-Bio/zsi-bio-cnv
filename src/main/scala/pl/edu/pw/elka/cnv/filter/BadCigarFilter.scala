package pl.edu.pw.elka.cnv.filter

import htsjdk.samtools.CigarOperator
import pl.edu.pw.elka.cnv.model.CNVRecord

/**
 * This read filter will filter out the following cases:
 * - different length and cigar length
 * - hard/soft clips in the middle of the cigar
 * - starting with deletions (with or without preceding clips)
 * - ending in deletions (with or without follow-up clips)
 * - fully hard or soft clipped
 * - consecutive indels in the cigar (II, DD, ID or DI)
 */
class BadCigarFilter extends ReadFilter {

  override def filterOut(read: CNVRecord): Boolean = {
    val cigar = read.getCigar
    val elemIter = cigar.getCigarElements.iterator

    if (cigar.isEmpty)
      return false
    if (read.getReadLength != cigar.getReadLength)
      return true

    var firstOp = CigarOperator.H
    while (elemIter.hasNext && (firstOp == CigarOperator.H || firstOp == CigarOperator.S)) {
      val op = elemIter.next.getOperator

      if (firstOp != CigarOperator.H && op == CigarOperator.H)
        return true

      firstOp = op
    }

    if (firstOp == CigarOperator.D)
      return true

    var lastOp = firstOp
    var previousOp = firstOp
    var hasMeaningfulElements = firstOp != CigarOperator.H && firstOp != CigarOperator.S
    var previousElementWasIndel = firstOp == CigarOperator.I

    while (elemIter.hasNext) {
      val op = elemIter.next.getOperator

      if (op != CigarOperator.S && op != CigarOperator.H) {
        if (previousOp == CigarOperator.S || previousOp == CigarOperator.H)
          return true

        lastOp = op

        if (!hasMeaningfulElements && op.consumesReadBases)
          hasMeaningfulElements = true

        if (op == CigarOperator.I || op == CigarOperator.D) {
          if (previousElementWasIndel)
            return true

          previousElementWasIndel = true
        } else
          previousElementWasIndel = false
      } else if (op == CigarOperator.S && previousOp == CigarOperator.H)
        return true

      previousOp = op
    }

    return lastOp == CigarOperator.D || !hasMeaningfulElements
  }

}

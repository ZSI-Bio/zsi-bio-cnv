package pl.edu.pw.elka.cnv.filter

import pl.edu.pw.elka.cnv.model.CNVRecord

/**
 * This filter recognizes the SAM flag corresponding to being unmapped. It is intended to ensure
 * that only reads that are likely to be mapped in the right place, and therefore to be informative,
 * will be used in analysis.
 */
class UnmappedReadFilter extends ReadFilter {

  override def filterOut(read: CNVRecord): Boolean =
    read.getReadUnmappedFlag || read.getAlignmentStart == 0

}

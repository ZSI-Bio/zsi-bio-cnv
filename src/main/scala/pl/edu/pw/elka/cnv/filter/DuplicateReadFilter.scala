package pl.edu.pw.elka.cnv.filter

import pl.edu.pw.elka.cnv.model.CNVRecord

/**
 * This filter recognizes the SAM flag corresponding to the duplication of a given read.
 */
class DuplicateReadFilter extends ReadFilter {

  override def filterOut(read: CNVRecord): Boolean =
    read.getDuplicateReadFlag

}

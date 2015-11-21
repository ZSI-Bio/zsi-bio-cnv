package pl.edu.pw.elka.cnv.filter

import pl.edu.pw.elka.cnv.model.CNVRecord

/**
 * This filter recognizes the SAM flag corresponding to the vendor quality check.
 */
class FailsVendorQualityCheckFilter extends ReadFilter {

  override def filterOut(read: CNVRecord): Boolean =
    read.getReadFailsVendorQualityCheckFlag

}

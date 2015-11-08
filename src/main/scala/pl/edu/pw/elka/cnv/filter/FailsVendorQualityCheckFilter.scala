package pl.edu.pw.elka.cnv.filter

import pl.edu.pw.elka.cnv.model.CNVRecord

/**
 * Created by mariusz-macbook on 08/11/15.
 */
class FailsVendorQualityCheckFilter extends ReadFilter {

  override def filterOut(read: CNVRecord): Boolean =
    read.getReadFailsVendorQualityCheckFlag

}

package pl.edu.pw.elka.cnv.filter

import pl.edu.pw.elka.cnv.model.CNVRecord

/**
 * Created by mariusz-macbook on 11/11/15.
 */
class BadMappingQualityFilter(minQuality: Int, maxQuality: Int) extends ReadFilter {

  override def filterOut(read: CNVRecord): Boolean =
    read.getMappingQuality < minQuality || read.getMappingQuality > maxQuality

}

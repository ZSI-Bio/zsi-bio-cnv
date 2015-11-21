package pl.edu.pw.elka.cnv.filter

import pl.edu.pw.elka.cnv.model.CNVRecord

/**
 * This filter recognizes the SAM flag corresponding to the mapping quality of a given read.
 *
 * @param minQuality Reads with lower mapping quality will be filtered out.
 * @param maxQuality Reads with greater mapping quality will be filtered out.
 */
class BadMappingQualityFilter(minQuality: Int, maxQuality: Int) extends ReadFilter {

  override def filterOut(read: CNVRecord): Boolean =
    read.getMappingQuality < minQuality || read.getMappingQuality > maxQuality

}

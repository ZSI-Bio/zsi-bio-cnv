package pl.edu.pw.elka.cnv.model

import htsjdk.samtools.Cigar

/**
 * Created by mariusz-macbook on 07/11/15.
 */
trait CNVRecord extends Serializable {

  def getReferenceName: Int

  def getAlignmentStart: Int

  def getAlignmentEnd: Int

  def getCigar: Cigar

}

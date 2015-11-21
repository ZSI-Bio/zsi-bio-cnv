package pl.edu.pw.elka.cnv.filter

import pl.edu.pw.elka.cnv.model.CNVRecord

/**
 * Base trait for read filters.
 */
trait ReadFilter extends Serializable {

  /**
   * Method that determines if given read should be filtered out.
   *
   * @param read Read to be analyzed.
   * @return Boolean flag.
   */
  def filterOut(read: CNVRecord): Boolean

}

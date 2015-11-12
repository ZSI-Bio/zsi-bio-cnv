package pl.edu.pw.elka.cnv.filter

import pl.edu.pw.elka.cnv.model.CNVRecord

/**
 * Created by mariusz-macbook on 08/11/15.
 */
trait ReadFilter extends Serializable {

  def filterOut(read: CNVRecord): Boolean

}

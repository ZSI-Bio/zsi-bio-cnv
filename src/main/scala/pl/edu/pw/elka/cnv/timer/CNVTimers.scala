package pl.edu.pw.elka.cnv.timer

import org.bdgenomics.utils.instrumentation.Metrics

/**
 * Created by mariusz-macbook on 03/01/16.
 */
object CNVTimers extends Metrics {

  val CoverageTimer = timer("Calculating Coverage")
  val RpkmTimer = timer("Calculating RPKMs")
  val ZrpkmTimer = timer("Calculating ZRPKMs")
  val SvdTimer = timer("Calculating SVD")
  val CallingTimer = timer("Calculating Calls")

}

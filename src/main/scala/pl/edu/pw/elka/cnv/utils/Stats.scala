package pl.edu.pw.elka.cnv.utils

trait Stats {

  def stddev(data: Array[Double]): Double = {
    val mean = data.sum / data.size
    data.map(x => (x - mean) * (x - mean)).sum / data.size
  }

  def median(data: Array[Double]): Double = {
    val (lower, upper) = data.sorted.splitAt(data.size / 2)
    if (data.size % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }

  def rpkm(count: Long, len: Long, total: Double): Double =
    return (1000000000 * count) / (len * total)

  def zrpkm(rpkm: Double, median: Double, stddev: Double): Double =
    return (rpkm - median) / stddev

}

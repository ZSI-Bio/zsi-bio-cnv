package pl.edu.pw.elka.cnv.conifer

trait CNVUtils {

  def stddev(data: Seq[Float]): Float = {
    val mean = data.sum / data.size
    data.map(x => (x - mean) * (x - mean)).sum / data.size
  }

  def median(data: Seq[Float]): Float = {
    val (lower, upper) = data.sorted.splitAt(data.size / 2)
    if (data.size % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }

  def rpkm(count: Long, len: Long, total: Float): Float =
    return (1000000000 * count) / (len * total)

  def zrpkm(rpkm: Float, median: Float, stddev: Float): Float =
    return (rpkm - median) / stddev

}

# ZSI-BIO-CNV

The goal of this project is to create a library with a fast and scalable implementation of the algorithms detecting CNV
(Copy Number Variation) mutations in a human genome. In addition, it contains many popular features used to analyze
genomic data, e.g. calculating coverage, RPKM, ZRPKM or SVD-ZRPKM values.

Technologies used:
* Apache Spark
* Hadoop-BAM

## Dependencies

In order to build the project you need to have the following dependencies installed:
* JVM 1.7
* Scala 2.10.4
* SBT 0.13.5

## Getting Started

1. Checkout the source using `git clone https://github.com/mkopacz/zsi-bio-cnv.git`
2. Build the package using `sbt package` from the project directory
3. Run the tests using `sbt test` from the project directory

If all tests are passed, the library is ready to be used.
If you want to generate an API documentation run `sbt doc` from the project directory.

## Examples

1. Calculating coverage:
```scala
val samples: Map[Int, String] = scanForSamples("path/to/bam/files")
val reads: RDD[(Int, SAMRecord)] = loadReads(sc, samples)
val bedFile: Broadcast[mutable.HashMap[Int, (Int, Int, Int)]] = sc.broadcast {
    readBedFile("path/to/bed/file")
}

val coverage: RDD[(Int, Iterable[(Int, Int)])] = {
    val counter = new CoverageCounter(sc, bedFile, reads)
    coverageToRegionCoverage(counter.calculateCoverage)
}
```
*Check out the API documentation for more information about various options of the coverage calculation.*

2. Calculating RPKM values:
```scala
// this is continuation of previous example

val rpkms: RDD[(Int, Array[Double])] = {
    val counter = new RpkmsCounter(reads, bedFile, coverage)
    counter.calculateRpkms
}
```

3. Calculating ZRPKM values:
```scala
// this is continuation of previous example

val zrpkms: RDD[(Int, Array[Double])] = {
    val counter = new ZrpkmsCounter(samples, rpkms, 1.0)
    counter.calculateZrpkms
}
```

4. Calculating SVD-ZRPKM values:
```scala
// this is continuation of previous example

val matrices: RDD[(Int, Array[Int], RealMatrix)] = {
    val counter = new SvdCounter(sc, samples, bedFile, zrpkms, 12)
    counter.calculateSvd
}
```

5. Making calls:
```scala
// this is continuation of previous example

val calls: RDD[(Int, Int, Int, Int, String)] = {
    val caller = new Caller(bedFile, matrices, 1.5)
    caller.call
}
```

6. CoNIFER algorithm:
```scala
val bedFilePath = "path/to/bed/file"
val bamFilesPath = "path/to/bam/files"

val conifer = new Conifer(sc, bedFilePath, bamFilesPath, 1.0, 1, 1.5)
conifer.calculate
```
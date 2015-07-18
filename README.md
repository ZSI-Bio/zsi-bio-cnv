# ZSI-BIO-CNV

The goal of this project is to create a library with a fast and scalable implementation of the algorithms detecting CNV
(Copy Number Variation) mutations in a human genome. In addition, it contains many popular features used to analyze
genomic data, e.g. calculating coverage or RPKM values.

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

If all tests are passed you are ready to go!

If you want to generate an API documentation run 'sbt doc' from the project directory.

## Examples

1. Calculating coverage:
```scala
import pl.edu.pw.elka.cnv.coverage.CoverageCounter
import pl.edu.pw.elka.cnv.utils.ConvertionUtils
import pl.edu.pw.elka.cnv.utils.FileUtils

object Example extends Application with ConvertionUtils with FileUtils {

    val samples = scanForSamples("path/to/bam/files")
    val reads = loadReads(sc, samples)
    val bedFile = readBedFile(sc, "path/to/bed/file")

    val coverage = {
        val counter = new CoverageCounter(sc, bedFile, reads)
        coverageToRegionCoverage(counter.calculateCoverage)
    }

}
```
*Check out the API documentation for more information about various options of the coverage calculation.*

2. Calculating RPKM values:
```scala
import pl.edu.pw.elka.cnv.coverage.CoverageCounter
import pl.edu.pw.elka.cnv.rpkm.RpkmsCounter
import pl.edu.pw.elka.cnv.utils.ConvertionUtils
import pl.edu.pw.elka.cnv.utils.FileUtils

object Example extends Application with ConvertionUtils with FileUtils {

    val samples = scanForSamples("path/to/bam/files")
    val reads = loadReads(sc, samples)
    val bedFile = readBedFile(sc, "path/to/bed/file")

    val coverage = {
        val counter = new CoverageCounter(sc, bedFile, reads)
        coverageToRegionCoverage(counter.calculateCoverage)
    }

    val rpkms = {
        val counter = new RpkmsCounter(reads, bedFile, coverage)
        counter.calculateRpkms
    }

}
```

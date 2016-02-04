import scala.util.Properties

name := """zsi-bio-cnv"""

version := "1.0"

scalaVersion := "2.10.4"

val DEFAULT_SPARK_VERSION = "1.6.0"
val DEFAULT_HADOOP_VERSION = "2.7.1"

lazy val sparkVersion = Properties.envOrElse("SPARK_VERSION", DEFAULT_SPARK_VERSION)
lazy val hadoopVersion = Properties.envOrElse("SPARK_HADOOP_VERSION", DEFAULT_HADOOP_VERSION)

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "3.0.0-M15" % "test",
  "org.apache.spark" % "spark-core_2.10" % sparkVersion % "provided",
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
  "org.bdgenomics.adam" % "adam-core_2.10" % "0.18.2"
    exclude("org.apache.spark", "spark-core_2.10")
    exclude("org.apache.hadoop", "hadoop-client")
    exclude("org.bdgenomics.utils", "utils-metrics_2.10"),
  "org.bdgenomics.utils" % "utils-misc_2.10" % "0.2.3"
    exclude("org.apache.spark", "spark-core_2.10")
    exclude("org.apache.hadoop", "hadoop-client"),
  "org.seqdoop" % "hadoop-bam" % "7.2.1"
    exclude("org.apache.hadoop", "hadoop-client"),
  "spark.jobserver" % "job-server-api_2.10" % "0.5.2"
)

resolvers ++= Seq(
  "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
)

parallelExecution in Test := false

assemblyJarName in assembly := "zsi-bio-cnv.jar"
mainClass in assembly := Some("pl.edu.pw.elka.cnv.Main")
assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "hadoop", "yarn", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", "commons", xs@_*) => MergeStrategy.first
  case ("git.properties") => MergeStrategy.concat
  case ("log4j.properties") => MergeStrategy.concat
  case x => (assemblyMergeStrategy in assembly).value(x)
}

lazy val copyDocAssetsTask = taskKey[Unit]("Copy doc assets")

copyDocAssetsTask := {
  val sourceDir = file("resources/doc-resources")
  val targetDir = (target in(Compile, doc)).value
  IO.copyDirectory(sourceDir, targetDir)
}

copyDocAssetsTask <<= copyDocAssetsTask triggeredBy (doc in Compile)

net.virtualvoid.sbt.graph.Plugin.graphSettings

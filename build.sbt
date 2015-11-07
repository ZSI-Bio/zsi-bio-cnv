name := """zsi-bio-cnv"""

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "2.2.5" % "test",
  "org.apache.spark" % "spark-core_2.10" % "1.4.1",
  "org.bdgenomics.adam" % "adam-core_2.10" % "0.18.1",
  "spark.jobserver" % "job-server-api_2.10" % "0.5.2"
)

resolvers ++= Seq(
  Classpaths.typesafeReleases, "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
)

parallelExecution in Test := false

lazy val copyDocAssetsTask = taskKey[Unit]("Copy doc assets")

copyDocAssetsTask := {
  val sourceDir = file("resources/doc-resources")
  val targetDir = (target in(Compile, doc)).value
  IO.copyDirectory(sourceDir, targetDir)
}

copyDocAssetsTask <<= copyDocAssetsTask triggeredBy (doc in Compile)

net.virtualvoid.sbt.graph.Plugin.graphSettings

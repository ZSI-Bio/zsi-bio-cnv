name := """zsi-bio-cnv"""

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "3.0.0-M9" % "test",
  "org.apache.spark" % "spark-core_2.10" % "1.5.1",
  "spark.jobserver" % "job-server-api_2.10" % "0.5.2",
  "org.apache.commons" % "commons-math3" % "3.5",
  "org.seqdoop" % "hadoop-bam" % "7.1.0"
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

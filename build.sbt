name := """zsi-bio-cnv"""

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "3.0.0-M9" % "test",
  "org.apache.spark" % "spark-core_2.10" % "1.5.1",
  "org.apache.commons" % "commons-math3" % "3.5",
  "org.seqdoop" % "hadoop-bam" % "7.1.0"
)

lazy val copyDocAssetsTask = taskKey[Unit]("Copy doc assets")

copyDocAssetsTask := {
  val sourceDir = file("resources/doc-resources")
  val targetDir = (target in(Compile, doc)).value
  IO.copyDirectory(sourceDir, targetDir)
}

copyDocAssetsTask <<= copyDocAssetsTask triggeredBy (doc in Compile)

parallelExecution in Test := false


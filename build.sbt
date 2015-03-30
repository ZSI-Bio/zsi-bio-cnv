name := """zsi-bio-cnv"""

version := "1.0"

scalaVersion := "2.11.5"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.apache.spark" %% "spark-core" % "1.2.1",
  "org.seqdoop" % "hadoop-bam" % "7.0.0"
)


name := "SparkDIMM"

version := "0.1"

scalaVersion := "2.13.16"  

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.4",
  "org.apache.spark" %% "spark-sql"  % "3.5.4",
  "org.apache.spark" %% "spark-mllib" % "3.5.4",
  "org.scalatest" %% "scalatest" % "3.2.16" % Test
 
)
name := "SparkDIMM"

version := "0.1"

// scalaVersion := "2.13.16"  
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.4",
  "org.apache.spark" %% "spark-sql"  % "3.5.4",
  "org.apache.spark" %% "spark-mllib" % "3.5.4",
  "org.scalatest" %% "scalatest" % "3.2.16" % Test
)

assembly / mainClass := Some("dimm.driver.IMMJob")

import sbtassembly.AssemblyPlugin.autoImport._

assembly / assemblyMergeStrategy := {
  case PathList("google", "protobuf", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", xs @ _*) =>
    xs.map(_.toLowerCase) match {
      case ("manifest.mf" :: Nil) => MergeStrategy.discard
      case ("index.list" :: Nil) => MergeStrategy.discard
      case ("dependencies" :: Nil) => MergeStrategy.discard
      case ps if ps.exists(_.endsWith(".sf")) => MergeStrategy.discard
      case ps if ps.exists(_.endsWith(".dsa")) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}
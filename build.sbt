name := "task_spark"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.16"

libraryDependencies += "org.apache.spark" %% "spark-core" % "4.0.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.0.0" % "provided"

libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.13.16" % "compile"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test

libraryDependencies += "com.typesafe" % "config" % "1.4.3"


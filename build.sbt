name := "mihir_kelkar_hw2"

version := "0.1"

scalaVersion := "2.13.3"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe" % "config" % "1.4.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.apache.hadoop" % "hadoop-common" % "3.2.1",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.2.1",
  "org.scala-lang.modules" %% "scala-xml" % "1.3.0"
)



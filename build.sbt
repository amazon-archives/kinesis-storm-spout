name := "kinesis-storm-spout"

version := "0.0.1"

scalaVersion := "2.10.4"

sbtVersion := "0.13.5"

libraryDependencies ++= Seq(
  "org.apache.storm" % "storm-core" % "0.9.2-incubating",
  "com.amazonaws" % "aws-java-sdk" % "1.8.9.1",
  "org.apache.commons" % "commons-lang3" % "3.3.2",
  "com.google.guava" % "guava" % "18.0",
  "com.netflix.curator" % "curator-framework" % "1.3.3"
)

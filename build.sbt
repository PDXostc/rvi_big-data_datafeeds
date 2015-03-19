import NativePackagerHelper._

name := "data-feeds"

organization := "com.advancedtelematic"

maintainer in Docker := "Advanced Telematic Systems <dev@advancedtelematic.com>"

version := "0.2.0"

scalaVersion := "2.11.6"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "spray repo" at "http://repo.spray.io"

val akkaVersion     = "2.3.9"

val SparkVersion = "1.3.0"

val spark = Seq(
  "com.datastax.spark"  %% "spark-cassandra-connector"  % "1.2.0-alpha3",
  "org.apache.spark" %% "spark-core" % SparkVersion,
  "org.apache.spark" %% "spark-streaming" % SparkVersion,
  "org.apache.spark"    %% "spark-mllib" % SparkVersion exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core")
)

val akka = Seq(
  "com.typesafe.akka" % "akka-stream-experimental_2.10" % "1.0-M3",
  "com.typesafe.akka"   %%  "akka-actor"       % akkaVersion,
  "com.typesafe.akka"   %%  "akka-slf4j"       % akkaVersion,
  "ch.qos.logback"      %   "logback-classic"  % "1.0.13"
)

libraryDependencies ++= spark ++ Seq(
  "com.github.nscala-time" %% "nscala-time" % "1.6.0",
  "org.apache.kafka" %% "kafka" % "0.8.2.1" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  ),
  "org.spire-math" %% "spire" % "0.9.0",
  "com.chuusai" %% "shapeless" % "2.1.0",
//  compilerPlugin("org.scalamacros" % "paradise_2.10.4" % "2.0.1"),
  "io.spray" %% "spray-client" % "1.3.2",
  "com.typesafe.play"   %%  "play-json"        % "2.3.4"
)

mainClass in (Compile) := Some("com.advancedtelematic.feed.Runner")

enablePlugins(JavaAppPackaging)

enablePlugins(DockerPlugin)

dockerBaseImage := "dockerfile/java:oracle-java8"

mappings in Universal ++= directory("data")

dockerRepository in Docker := Some("advancedtelematic")

packageName in Docker := "rvi_data_feeds"

dockerUpdateLatest in Docker := true

name := "data-feeds"

organization := "com.advancedtelematic"

version := "0.0.1"

scalaVersion := "2.10.4"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"

val akkaVersion     = "2.3.9"

val akka = Seq(
  "com.typesafe.akka"   %%  "akka-actor"       % akkaVersion,
  "com.typesafe.akka"   %%  "akka-slf4j"       % akkaVersion,
  "ch.qos.logback"      %   "logback-classic"  % "1.0.13"
)

libraryDependencies ++= akka ++ Seq(
  "com.github.nscala-time" %% "nscala-time" % "1.6.0",
  "org.apache.kafka" %% "kafka" % "0.8.1" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  )
)

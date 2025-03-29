import sbt.Keys._
import sbt._

import sbtassembly.AssemblyPlugin.autoImport._

name := "orinoco"


version := "5.17.3"

scalaVersion := "2.12.14"

lazy val versions = new {
  val spark = "3.4.4"
  val log4j = "1.7.30"
  val geoip2 = "2.5.0"
  val typesafeConfig = "1.4.0"
//  val statusUpdater = "0.1.4
  val rql = "1.1.2"
  val json4s = "3.6.7"
  val scopt = "3.6.0"
  val ratHiveCommon = "0.13.5"
  val scalaTest = "3.0.3"
  val googleCloudStorage = "2.39.0"
  val commonsMath3 = "3.6.1"

}

lazy val root = (project in file("."))
  .configs(Test, IntegrationTest)
  .settings(
    fork := false,
    parallelExecution := false,
    javaOptions ++= Seq(
      "-Xms512M",          // Initial heap size for SBT itself
      "-Xmx2G",            // Maximum heap size for SBT itself
      "-XX:MaxMetaspaceSize=2G",
      "-XX:+UseG1GC",
      "-XX:InitiatingHeapOccupancyPercent=35",
      "-XX:+CMSClassUnloadingEnabled",
      "-Djava.net.preferIPv4Stack=true"
    ),
    scalacOptions ++= Seq(
      "-Ywarn-unused:imports",
      "-deprecation",
      "-feature",
      "-unchecked"
    ),

    //enables "debug" output mode
    Test / testOptions   += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
  )

libraryDependencies ++= Seq(
  "com.rakuten.global.rat" % "rat-hive-common" % versions.ratHiveCommon
)
  .map(_.excludeAll(
    ExclusionRule("org.apache.hadoop", "hadoop-core"),
    ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm")
  ))

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-lang3" % "3.10",
  "org.apache.commons" % "commons-math3" % versions.commonsMath3,
  "org.apache.hadoop" % "hadoop-client" % "3.2.0" % "provided",
  "org.apache.spark" %% "spark-core" % versions.spark % "provided",
  "org.apache.spark" %% "spark-sql" % versions.spark % "provided",
  "org.slf4j" % "slf4j-api" % versions.log4j,
  "com.maxmind.geoip2" % "geoip2" % versions.geoip2, // 2.5.0 required for spark-compatibility
  "com.typesafe" % "config" % versions.typesafeConfig,
  "org.json4s" %% "json4s-native" % versions.json4s,
  "org.json4s" %% "json4s-core" % versions.json4s,
  "org.json4s" %% "json4s-jackson" % versions.json4s,
  "com.github.scopt" %% "scopt" % versions.scopt,
  "org.scalatest" %% "scalatest" % versions.scalaTest % Test,
  //"com.rakuten.global.rat" % "rat-hive-common" % versions.ratHiveCommon
  "com.google.cloud" % "google-cloud-storage" % versions.googleCloudStorage
)

resolvers ++= Seq(
  ("Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases")
    .withAllowInsecureProtocol(allowInsecureProtocol = true),
  ("SuperConJars" at "https://conjars.wensel.net/repo/")
    .withAllowInsecureProtocol(allowInsecureProtocol = true)
)

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

compileScalastyle := (Compile / scalastyle).toTask("").value
(Compile / compile) := ((Compile / compile) dependsOn compileScalastyle).value

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("org.json4s.**" -> "shaded.org.json4s.@1").inAll
)

// Prevent including Scala library and reflect multiple times
assembly / assemblyOption := (assemblyOption in assembly).value.copy(includeScala = false)

dependencyOverrides ++= Seq(
  "org.json4s" %% "json4s-native" % "3.6.7",
  "org.json4s" %% "json4s-core" % "3.6.7",
  "org.json4s" %% "json4s-jackson" % "3.6.7"
)
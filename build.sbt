import Resolvers._
import Dependencies._
import sbt._
import Keys._

resolvers ++= Seq(
  clojars,
  maven_local,
  snapshot_remote1
)


libraryDependencies ++= Seq(
  //    cassandra.exclude("com.carrotsearch", "hppc"),
  //    kafka,
  //    elasticsearch,
      (spark_core).exclude("net.java.dev.jets3t", "jets3t"),
  //    (spark_cassandra_connector).exclude("org.apache.cassandra", "cassandra-clientutil"),
      spark_sql,
      spark_streaming ,
      spark_mllib,
      spark_hive,
      scalatest
  //    spark_streaming_kafka,
  //    mysql_connector_java,
  //    mysql_connector_mxj,
  //    mysql_connector_mxj_db_file

)

//name := "SparkTwoExperiments"
//
//version := "1.0"
//
//lazy val commonSettings = Seq(
//  organization := "com.zhuohuawu",
//  //scalaVersion := "2.11.6",
//  resolvers ++= Seq(
//    clojars,
////    viafoura_maven,
////    viafoura_maven_snapshot,
//    maven_local
//  ),
//  dependencyOverrides ++=  Set(
//    "org.apache.commons" % "commons-lang3" % "3.3.2",
//    "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4",
//    "commons-beanutils" % "commons-beanutils" % "1.9.2"
//  ),
//  test in assembly := {},
//  assemblyMergeStrategy in assembly := {
//    {
//      case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
//      case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
//      case PathList("javax", "annotation", xs @ _*) => MergeStrategy.last
//      case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
//      case PathList("org", "apache", "commons", "beanutils", xs @ _*) => MergeStrategy.last
//      case PathList("org", "apache", "commons", "shadebeanutils", xs @ _*) => MergeStrategy.first
//      case PathList("org", "apache", "commons", "logging", xs @ _*) => MergeStrategy.first
//      case PathList("org", "apache", "commons", "collections", xs @ _*) => MergeStrategy.first
//      case PathList("org", "apache", "hadoop", "fs", xs @ _*) => MergeStrategy.last
//      case PathList("org", "apache", "hadoop", "yarn", xs @ _*) => MergeStrategy.last
//      case PathList("com", "google", xs @ _*) => MergeStrategy.last
//      case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
//      case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
//      case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
//      case PathList("org", "slf4j", xs @ _*) => MergeStrategy.last
//      case PathList("org", "joda", "time", "base", xs @ _*) => MergeStrategy.last
//      case PathList("org", "tartarus", "snowball", xs @ _*) => MergeStrategy.last
//      case "about.html" => MergeStrategy.rename
//      case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
//      case "META-INF/mailcap" => MergeStrategy.last
//      case "META-INF/mimetypes.default" => MergeStrategy.last
//      case "META-INF/io.netty.versions.properties" => MergeStrategy.last
//      case "plugin.properties" => MergeStrategy.last
//      case "log4j.properties" => MergeStrategy.last
//      case "stylesheet.css" => MergeStrategy.last
//      case "mime.types" => MergeStrategy.last
//      case "META-INF/eclipse.inf" => MergeStrategy.last
//      case "PropertyList-1.0.dtd" => MergeStrategy.last
//      case "properties.dtd" => MergeStrategy.last
//      case "vfmetrics.properties" => MergeStrategy.last
//      case x => val oldStrategy = (assemblyMergeStrategy in assembly).value
//
//      oldStrategy(x)
//    }
//  },
//
//  assemblyShadeRules in assembly := Seq(
//    ShadeRule.rename("org.apache.commons.beanutils.**" -> "org.apache.commons.shadebeanutils.@1").inAll,
//    ShadeRule.rename("org.apache.http.**" -> "org.apache.shadehttp.@1").inAll
//  ),
//
//  assemblyExcludedJars in assembly := {
//    val cp = (fullClasspath in assembly).value
//    cp.filter(file => {
//      file.data.getName == "metrics-core-3.0.1.jar" || file.data.getName == "commons-beanutils-1.8.0.jar"
//    })
//  }
//
//)
//
//lazy val root = project in file(".")
//
////version:="0.4.7"
//lazy val common = (project in file("common")).settings(commonSettings: _*).settings(
//  name := "viafoura-analytics-common",
//  version:="0.0.1",
//  libraryDependencies ++= (default_dependencies_seq ++ Seq(scala_compiler)),
//  libraryDependencies in Test ++= default_dependencies_seq_in_test
//)
//
//lazy val embedded = (project in file("embedded")).dependsOn(common).settings(commonSettings: _*).settings(
//  name := "embedded",
//  version := "0.0.1",
//  libraryDependencies ++= Seq(
////    cassandra.exclude("com.carrotsearch", "hppc"),
////    kafka,
////    elasticsearch,
//    (spark_core).exclude("net.java.dev.jets3t", "jets3t"),
////    (spark_cassandra_connector).exclude("org.apache.cassandra", "cassandra-clientutil"),
//    spark_streaming ,
//    spark_hive
////    spark_streaming_kafka,
////    mysql_connector_java,
////    mysql_connector_mxj,
////    mysql_connector_mxj_db_file
//  )
//)


name := "SparkTwoExperiments"

version := "1.0"
//
scalaVersion := "2.11.6"
//
//val sparkVersion = "2.0.1"


//resolvers ++= Seq(
//  "apache-snapshots" at "http://repository.apache.org/snapshots/"
//)
//
//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
//  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
//  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
//  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
//  "org.apache.spark" %% "spark-hive" % sparkVersion% "provided",
//  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
//)
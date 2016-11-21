import sbt._

object Dependencies {
  val scala_main_version = "2.11"
  val spark_version = "2.0.1"

  val scala_compiler = "org.scala-lang" % "scala-compiler" % "2.11.6"

  val spark_core = ("org.apache.spark" % s"spark-core_${scala_main_version}" % spark_version)
    .exclude("org.mortbay.jetty", "servlet-api")
    .exclude("commons-beanutils", "commons-beanutils-core")
    .exclude("commons-collections", "commons-collections")
    //.exclude("commons-logging", "commons-logging")
    .exclude("com.esotericsoftware.minlog", "minlog")

  val spark_streaming = "org.apache.spark" % s"spark-streaming_${scala_main_version}" % spark_version

  val spark_sql = ("org.apache.spark" % s"spark-sql_${scala_main_version}" % spark_version)
  val spark_mllib = ("org.apache.spark" % s"spark-mllib_${scala_main_version}" % spark_version)
  val spark_hive = ("org.apache.spark" % s"spark-hive_${scala_main_version}" % spark_version)
  val scalatest = "org.scalatest" % "scalatest_2.11" % "2.2.6"
  val junit = "junit" % "junit" % "4.12"
  val log4j = "log4j" % "log4j" % "1.2.17.redhat-1"


  val spark_streaming_kafka =   ("org.apache.spark" % s"spark-streaming-kafka_${scala_main_version}" % "1.6.2")
    .exclude("org.spark-project.spark", "unused")

  val spark_cassandra_connector = ("com.datastax.spark" % s"spark-cassandra-connector_${scala_main_version}" % "1.6.2")
    .exclude("com.codahale.metrics", "metrics-core")
    .exclude("io.netty", "netty-handler")
    .exclude("io.netty", "netty-buffer")
    .exclude("io.netty", "netty-common")
    .exclude("io.netty", "netty-transport")
    .exclude("io.netty", "netty-codec")

  val google_guice = ("com.google.inject" % "guice" % "4.0")
    .exclude("com.google.guava", "guava")
    .exclude("stax", "stax-api")

  val uadetector_core = "net.sf.uadetector" % "uadetector-core" % "0.9.22"
  val uadetector_resources = "net.sf.uadetector" % "uadetector-resources" % "2014.10"
  val ua_parser = "ua_parser" % "ua-parser" % "1.3.1-SNAPSHOT"
  val zookeeper = ("org.apache.zookeeper" % "zookeeper" % "3.4.6")
    .exclude("org.slf4j", "slf4j-log4j12")

  val aspectj = "org.aspectj" % "aspectjrt" % "1.8.9"
  val hadoop_aws = ("org.apache.hadoop" % "hadoop-aws" % "2.7.3")
    .exclude("org.apache.hadoop", "hadoop-common")

  val quartz = "org.quartz-scheduler" % "quartz" % "2.2.2"
  val twitter_algebird = "com.twitter" % "algebird-core_2.11" % "0.11.0"

  val elasticsearch_spark = "org.elasticsearch" % "elasticsearch-spark_2.11" % "2.4.0"
  val elasticsearch = "org.elasticsearch" % "elasticsearch" % "2.4.1"
  val apache_commons_cli = "commons-cli" % "commons-cli" % "1.3.1"

  val cassandra = ("org.apache.cassandra" % "cassandra-all" % "3.5")
    .exclude("org.slf4j", "log4j-over-slf4j")
    .exclude("org.slf4j", "slf4j-api")

  val kafka = ("org.apache.kafka" % "kafka_2.11" % "0.8.2.1")

  val mysql_connector_java = "mysql" % "mysql-connector-java" % "5.1.17"
  val mysql_connector_mxj =  "mysql" % "mysql-connector-mxj" % "5.0.12"
  val mysql_connector_mxj_db_file = "mysql" % "mysql-connector-mxj-db-files" % "5.0.12"

//  val viafoura_monitoring = ("com.viafoura.common" % "common-monitoring" % "1.1")
//    .exclude("com.amazonaws", "aws-java-sdk-core")
//    .exclude("com.amazonaws", "aws-java-sdk-cloudwatch")

  val apache_http_client = "org.apache.httpcomponents" % "httpclient" % "4.5.2"

  val spark_testing_base = "com.holdenkarau" % "spark-testing-base_2.11" % "1.6.1_0.3.2"

  val default_dependencies_seq = Seq(
    (spark_core)
      .exclude("net.java.dev.jets3t", "jets3t"), // % Provided,

    spark_streaming, // % Provided ,
    spark_sql,
    spark_hive,
    spark_cassandra_connector,
    spark_streaming_kafka,
    spark_testing_base,
    google_guice,
    ua_parser,
    uadetector_core,
    uadetector_resources,
    zookeeper,
    aspectj,
    hadoop_aws,
    quartz,
    twitter_algebird,
//    viafoura_monitoring,
    scalatest % Test,
    junit  % Test
  )

  val default_dependencies_seq_in_test = Seq(
    (spark_core)
      .exclude("net.java.dev.jets3t", "jets3t"),

    spark_streaming,
    spark_sql,
    spark_hive,
    spark_cassandra_connector,
    spark_streaming_kafka,
    spark_testing_base,
    google_guice,
    ua_parser,
    uadetector_core,
    uadetector_resources,
    zookeeper,
    aspectj,
    hadoop_aws,
    quartz,
    twitter_algebird,
//    viafoura_monitoring,
    scalatest % Test,
    junit  % Test
  )
}

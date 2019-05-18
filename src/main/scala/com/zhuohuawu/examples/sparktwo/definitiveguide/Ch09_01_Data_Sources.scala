package com.zhuohuawu.examples.sparktwo.definitiveguide

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Spark Session example
  *
  */


object Ch09_01_Data_Sources {

  val logger = Logger.getLogger(Ch09_01_Data_Sources.getClass)

  def main(args: Array[String]) {

    logger.error("Beginning of SparkSessionExample.")

    val BASEPATH = "/Users/zhuohuawu/Documents/data/spark-definitive-guide"
    val BASE_OUTPUT_PATH = "/Users/zhuohuawu/Documents/zw_codes/GitLab/spark2_codes/spark2.0-examples/output/definitiveguide"

    val spark = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    import spark.implicits._

//    val dataframe =Seq(
//      (0, "Masters", "School of Information", "UC Berkeley"),
//      (2, "Masters", "EECS", "UC Berkeley"),
//      (1, "Ph.D.", "EECS", "UC Berkeley"))
//      .toDF("id", "degree", "department", "school")
//
//    // in Scala
//    dataframe.write
//
//
//    // COMMAND ----------
//
//    // in Scala
//    dataframe.write.format("csv")
//      .option("mode", "OVERWRITE")
//      .option("dateFormat", "yyyy-MM-dd")
//      .option("path", "output/definitiveguide/ch09-data-sources")
//      .save()
//
//
//    // COMMAND ----------
//
//    spark.read.format("csv")

//
//    // COMMAND ----------
//
//    // in Scala
//    spark.read.format("csv")
//      .option("header", "true")
//      .option("mode", "FAILFAST")
//      .option("inferSchema", "true")
//      .load("some/path/to/file.csv")
//
//
    // COMMAND ----------

    // in Scala
    import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
    val myManualSchema1 = new StructType(Array(
      new StructField("DEST_COUNTRY_NAME", StringType, true),
      new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      new StructField("count", LongType, false)
    ))
    spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(myManualSchema1)
      .load(BASEPATH + "/data/flight-data/csv/2010-summary.csv")
      .show(5)


    // COMMAND ----------

    // in Scala
    val myManualSchema = new StructType(Array(
      new StructField("DEST_COUNTRY_NAME", StringType, true),
      new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      new StructField("count", LongType, false) ))

    spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load(BASEPATH + "/data/flight-data/csv/2010-summary.csv")
      .take(5)


    // COMMAND ----------

    // in Scala
    val csvFile = spark.read.format("csv")
      .option("header", "true").option("mode", "FAILFAST").schema(myManualSchema)
      .load(BASEPATH + "/data/flight-data/csv/2010-summary.csv")


    // COMMAND ----------

    // in Scala
    csvFile.write.format("csv").mode("overwrite").option("sep", "\t")
      .save(BASE_OUTPUT_PATH + "/tmp/my-tsv-file.tsv")


    // COMMAND ----------

    spark.read.format("json")


    // COMMAND ----------

    // in Scala
    spark.read.format("json").option("mode", "FAILFAST").schema(myManualSchema)
      .load(BASEPATH + "/data/flight-data/json/2010-summary.json").show(5)


    // COMMAND ----------

    // in Scala
    csvFile.write.format("json").mode("overwrite").save("/tmp/my-json-file.json")


    // COMMAND ----------

    spark.read.format("parquet")


    // COMMAND ----------

    spark.read.format("parquet")


    // COMMAND ----------

    // in Scala
    spark.read.format("parquet")
      .load(BASEPATH + "/data/flight-data/parquet/2010-summary.parquet").show(5)


    // COMMAND ----------

    // in Scala
    csvFile.write.format("parquet").mode("overwrite")
      .save(BASE_OUTPUT_PATH + "/tmp/my-parquet-file.parquet")


    // COMMAND ----------

    // in Scala
    spark.read.format("orc").load(BASEPATH + "/data/flight-data/orc/2010-summary.orc").show(5)


    // COMMAND ----------

    // in Scala
    csvFile.write.format("orc").mode("overwrite").save(BASE_OUTPUT_PATH + "/tmp/my-json-file.orc")


    // COMMAND ----------

    // in Scala
    val driver =  "org.sqlite.JDBC"
    val path = BASEPATH + "/data/flight-data/jdbc/my-sqlite.db"
    val url = s"jdbc:sqlite:/${path}"
    val tablename = "flight_info"


    // COMMAND ----------

    import java.sql.DriverManager
    val connection = DriverManager.getConnection(url)
    connection.isClosed()
    connection.close()


    // COMMAND ----------

    // in Scala
    val dbDataFrame1 = spark.read.format("jdbc").option("url", url)
      .option("dbtable", tablename).option("driver",  driver).load()


    // COMMAND ----------

    // in Scala
    val pgDF = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://database_server")
      .option("dbtable", "schema.tablename")
      .option("user", "username").option("password","my-secret-password").load()


    // COMMAND ----------

    dbDataFrame1.select("DEST_COUNTRY_NAME").distinct().show(5)


    // COMMAND ----------

    dbDataFrame1.select("DEST_COUNTRY_NAME").distinct().explain


    // COMMAND ----------

    // in Scala
    dbDataFrame1.filter("DEST_COUNTRY_NAME in ('Anguilla', 'Sweden')").explain


    // COMMAND ----------

    // in Scala
    val pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
  AS flight_info"""
    val dbDataFrame = spark.read.format("jdbc")
      .option("url", url).option("dbtable", pushdownQuery).option("driver",  driver)
      .load()


    // COMMAND ----------

    dbDataFrame1.explain()


    // COMMAND ----------

    // in Scala
    val dbDataFrame2 = spark.read.format("jdbc")
      .option("url", url).option("dbtable", tablename).option("driver", driver)
      .option("numPartitions", 10).load()


    // COMMAND ----------

    dbDataFrame2.select("DEST_COUNTRY_NAME").distinct().show()


    // COMMAND ----------

    // in Scala
    val props2 = new java.util.Properties
    props2.setProperty("driver", "org.sqlite.JDBC")
    val predicates2 = Array(
      "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
      "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'")
    spark.read.jdbc(url, tablename, predicates2, props2).show()
    spark.read.jdbc(url, tablename, predicates2, props2).rdd.getNumPartitions // 2


    // COMMAND ----------

    // in Scala
    val props = new java.util.Properties
    props.setProperty("driver", "org.sqlite.JDBC")
    val predicates = Array(
      "DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'",
      "DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'")
    spark.read.jdbc(url, tablename, predicates, props).count() // 510


    // COMMAND ----------

    // in Scala
    val colName = "count"
    val lowerBound = 0L
    val upperBound = 348113L // this is the max count in our database
    val numPartitions = 10


    // COMMAND ----------

    // in Scala
    spark.read.jdbc(url,tablename,colName,lowerBound,upperBound,numPartitions,props)
      .count() // 255


    // COMMAND ----------

    // in Scala
    val newPath = "jdbc:sqlite://tmp/my-sqlite.db"
    csvFile.write.mode("overwrite").jdbc(newPath, tablename, props)


    // COMMAND ----------

    // in Scala
    spark.read.jdbc(newPath, tablename, props).count() // 255


    // COMMAND ----------

    // in Scala
    csvFile.write.mode("append").jdbc(newPath, tablename, props)


    // COMMAND ----------

    // in Scala
    spark.read.jdbc(newPath, tablename, props).count() // 765


    // COMMAND ----------

    spark.read.textFile(BASEPATH + "/data/flight-data/csv/2010-summary.csv")
      .selectExpr("split(value, ',') as rows").show()


    // COMMAND ----------

    csvFile.select("DEST_COUNTRY_NAME").write.text(BASE_OUTPUT_PATH + "/tmp/simple-text-file.txt")


    // COMMAND ----------

    // in Scala
    csvFile.limit(10).select("DEST_COUNTRY_NAME", "count")
      .write.partitionBy("count").text(BASE_OUTPUT_PATH + "/tmp/five-csv-files2.csv")


    // COMMAND ----------

    // in Scala
    csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME")
      .save(BASE_OUTPUT_PATH + "/tmp/partitioned-files.parquet")


    // COMMAND ----------

    val numberBuckets = 10
    val columnToBucketBy = "count"

    csvFile.write.format("parquet").mode("overwrite")
      .bucketBy(numberBuckets, columnToBucketBy).saveAsTable("bucketedFiles")


    // COMMAND ----------



    // COMMAND ----------
    val aaa = 1

  }

}

package com.zhuohuawu.examples.sparktwo.nb2code

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Spark Session example
  *
  */

// https://community.cloud.databricks.com/?o=8920468172695095#notebook/1809612576129460/command/1809612576129612
// convert from 02-DE-Pageviews
// download source file: http://datahub.io/en/dataset/english-wikipedia-pageviews-by-second
object NB_20160606_03_Wikipedia_DA_Pageviews {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger(NB_20160606_03_Wikipedia_DA_Pageviews.getClass).setLevel(Level.ERROR)
  val logger = Logger.getLogger(NB_20160606_03_Wikipedia_DA_Pageviews.getClass)

  //  val spark = getSparkSession()
  //  val sc = spark.sparkContext
  //  val sqlContext = spark.sqlContext
  //  val ssc = new StreamingContext(sc, Seconds(2))

  def main2(args: Array[String]): Unit = {

    logger.error("Beginning of NB_20160606_02_Wikipedia_DE_Pageviews.")

    val spark = getSparkSession()

    val sqlDeletePowerPlantPredictions = "DROP TABLE IF EXISTS power_plant_predictions"

    val sqlCreatePowerPlantPredictions = """CREATE TABLE power_plant_predictions(
      AT Double,
      V Double,
      AP Double,
      RH Double,
      PE Double,
      Predicted_PE Double
    )
    """

    println("sqlCreatePowerPlantPredictions is executed: ")
    spark.sql(sqlDeletePowerPlantPredictions)
    spark.sql(sqlCreatePowerPlantPredictions)
  }

  def getSparkSession(): SparkSession = {
    val spark = SparkSession.builder.
      master("local[4]")
      .appName("NB_20160606_02_Wikipedia_DE_Pageviews")
      .getOrCreate()
    return spark
  }

  def main(args: Array[String]) {

    logger.error("Beginning of NB_20160606_03_Wikipedia_DA_Pageviews.")

    val spark = getSparkSession()

    //val inputPath = "/Users/kevinwu/Documents/data/wikipedia/large/pageviews_by_second.tsv"
    val inputPath = "src/main/resources/wikipedia/small/pageviews_by_second.tsv"

    val rawTextDF = readInputData(spark, inputPath)

    println("hello, end3")
  }

  /**
    * read data from raw input path into a dataframe
    * @param spark
    * @param inputPath
    * @return
    */
  def readInputData(spark: SparkSession, inputPath: String) = {
    println("Begin to readInputData()")
    //val inputPath = "src/main/resources/power-plant/data/"


    val tempDf = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")        // Use first line of all files as header
      .option("inferSchema", "true")   // Automatically infer data types
      .option("delimiter", "\t")       // Use tab delimiter (default is comma-separator)
      .load(inputPath)

    val sqlContext = spark.sqlContext

    val PAGEVIEWS_BY_SECOND = "pageviews_by_second"

    tempDf.show(5, false)
    tempDf.createOrReplaceTempView(PAGEVIEWS_BY_SECOND)

    val pageviewsDF = spark.read.table(PAGEVIEWS_BY_SECOND)
    pageviewsDF.show(10, false)
    pageviewsDF.printSchema

    println("partition size = " + pageviewsDF.rdd.partitions.size)

    //println("count = " + pageviewsDF.count)

    //pageviewsDF.orderBy("timestamp").show(10)  // action
    // or the following, $"timestamp" not working???
    pageviewsDF.orderBy(pageviewsDF("timestamp")).show(10, false)  // action

    pageviewsDF.orderBy(pageviewsDF("timestamp"), pageviewsDF("site").desc).show(10, false)

    println("==== start pageviewsDF.count without cache: ")
    sqlContext.uncacheTable(PAGEVIEWS_BY_SECOND)
    val t1_start = System.currentTimeMillis()
    pageviewsDF.count
    val t1_end = System.currentTimeMillis()
    println("end pageviewsDF.count without cache, seconds = " + (t1_end - t1_start) * 1.0 / 1000)


    println("==== start pageviewsDF.count with cache: ")
    sqlContext.cacheTable(PAGEVIEWS_BY_SECOND)
    val t2_start = System.currentTimeMillis()
    pageviewsDF.count
    val t2_end = System.currentTimeMillis()
    println("end pageviewsDF.count with cache, seconds = " + (t2_end - t2_start) * 1.0 / 1000)

    println("==== start How many rows refer to mobile: ")

    val mobileCount = pageviewsDF.filter(pageviewsDF("site") === "mobile").count
    val desktopCount = pageviewsDF.filter(pageviewsDF("site") === "desktop").count

    println(s"mobileCount = $mobileCount" )
    println(s"desktopCount = $desktopCount" )

    // pageviewsDF.unpersist
    val pageviewsOrderedDF = spark.read.table("pageviews_by_second").orderBy("timestamp", "site").cache

    val partitionNumber1 = sqlContext.getConf("spark.sql.shuffle.partitions")

    println(s"before: partitionNumber = $partitionNumber1")

    sqlContext.setConf("spark.sql.shuffle.partitions", "3")
    val partitionNumber2 = sqlContext.getConf("spark.sql.shuffle.partitions")
    println(s"after: partitionNumber = $partitionNumber2")

    println("End to readInputData()")
    tempDf
  }

}


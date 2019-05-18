package com.zhuohuawu.examples.sparktwo.definitiveguide

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Spark Session example
  *
  */

/** TODO not tested

Error:(41, 31) Unable to find encoder for type stored in a Dataset.  Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases.
    val flights = flightsDF.as[Flight]

  */

//object Ch10_01_Spark_sql {
//
//  val logger = Logger.getLogger(Ch10_01_Spark_sql.getClass)
//
//  def main(args: Array[String]) {
//
//    logger.error("Beginning of SparkSessionExample.")
//
//    val BASEPATH = "/Users/zhuohuawu/Documents/data/spark-definitive-guide"
//    val BASE_OUTPUT_PATH = "/Users/zhuohuawu/Documents/zw_codes/GitLab/spark2_codes/spark2.0-examples/output/definitiveguide"
//
//    val spark = SparkSession.builder.
//      master("local")
//      .appName("spark session example")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    case class Flight(DEST_COUNTRY_NAME: String,
//                      ORIGIN_COUNTRY_NAME: String, count: BigInt)
//
//
//    // COMMAND ----------
//
//    val flightsDF = spark.read
//      .parquet(BASEPATH + "/data/flight-data/parquet/2010-summary.parquet/")
//    val flights = flightsDF.as[Flight]
//
//
//    // COMMAND ----------
//
//    flights.show(2)
//
//
//    // COMMAND ----------
//
//    flights.first.DEST_COUNTRY_NAME // United States
//
//
//    // COMMAND ----------
//
//    def originIsDestination(flight_row: Flight): Boolean = {
//      return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
//    }
//
//
//    // COMMAND ----------
//
//    flights.filter(flight_row => originIsDestination(flight_row)).first()
//
//
//    // COMMAND ----------
//
//    flights.collect().filter(flight_row => originIsDestination(flight_row))
//
//
//    // COMMAND ----------
//
//    val destinations = flights.map(f => f.DEST_COUNTRY_NAME)
//
//
//    // COMMAND ----------
//
//    val localDestinations = destinations.take(5)
//
//
//    // COMMAND ----------
//
//    case class FlightMetadata(count: BigInt, randomData: BigInt)
//
//    val flightsMeta = spark.range(500).map(x => (x, scala.util.Random.nextLong))
//      .withColumnRenamed("_1", "count").withColumnRenamed("_2", "randomData")
//      .as[FlightMetadata]
//
//
//    // COMMAND ----------
//
//    val flights1 = flights
//      .joinWith(flightsMeta, flights.col("count") === flightsMeta.col("count"))
//
//
//    // COMMAND ----------
//
//    flights1.selectExpr("_1.DEST_COUNTRY_NAME")
//
//
//    // COMMAND ----------
//
//    flights1.take(2)
//
//
//    // COMMAND ----------
//
//    val flights2 = flights.join(flightsMeta, Seq("count"))
//    flights2.show(false)
//
//    // COMMAND ----------
//
//    val flights3 = flights.join(flightsMeta.toDF(), Seq("count"))
//    flights3.show(false)
//
//    // COMMAND ----------
//
//    flights.groupBy("DEST_COUNTRY_NAME").count()
//
//
//    // COMMAND ----------
//
//    flights.groupByKey(x => x.DEST_COUNTRY_NAME).count()
//
//
//    // COMMAND ----------
//
//    flights.groupByKey(x => x.DEST_COUNTRY_NAME).count().explain
//
//
//    // COMMAND ----------
//
//    def grpSum(countryName:String, values: Iterator[Flight]) = {
//      values.dropWhile(_.count < 5).map(x => (countryName, x))
//    }
//    flights.groupByKey(x => x.DEST_COUNTRY_NAME).flatMapGroups(grpSum).show(5)
//
//
//    // COMMAND ----------
//
//    def grpSum2(f:Flight):Integer = {
//      1
//    }
//    flights.groupByKey(x => x.DEST_COUNTRY_NAME).mapValues(grpSum2).count().take(5)
//
//
//    // COMMAND ----------
//
//    def sum2(left:Flight, right:Flight) = {
//      Flight(left.DEST_COUNTRY_NAME, null, left.count + right.count)
//    }
//    flights.groupByKey(x => x.DEST_COUNTRY_NAME).reduceGroups((l, r) => sum2(l, r))
//      .take(5)
//
//
//    // COMMAND ----------
//
//    flights.groupBy("DEST_COUNTRY_NAME").count().explain
//
//
//    // COMMAND ----------
//
//
//
//    // COMMAND ----------
//    val aaa = 1
//
//  }
//
//}

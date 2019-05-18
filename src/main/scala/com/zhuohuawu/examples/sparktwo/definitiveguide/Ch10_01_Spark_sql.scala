package com.zhuohuawu.examples.sparktwo.definitiveguide

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Spark Session example
  *
  */

object Ch10_01_Spark_sql {

  val logger = Logger.getLogger(Ch10_01_Spark_sql.getClass)

  def main(args: Array[String]) {

    logger.error("Beginning of SparkSessionExample.")

    val BASEPATH = "/Users/zhuohuawu/Documents/data/spark-definitive-guide"
    val BASE_OUTPUT_PATH = "/Users/zhuohuawu/Documents/zw_codes/GitLab/spark2_codes/spark2.0-examples/output/definitiveguide"

    val spark = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    spark.sql("SELECT 1 + 1").show()


//    // COMMAND ----------
//
//    // in Scala
//    spark.read.json(BASEPATH + "/data/flight-data/json/2015-summary.json")
//      .createOrReplaceTempView("some_sql_view") // DF => SQL
//
//    spark.sql("""
//                SELECT DEST_COUNTRY_NAME, sum(count)
//                FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
//                """)
//      .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")
//      .count() // SQL => DF


    // COMMAND ----------

    val flights = spark.read.format("json")
      .load(BASEPATH + "/data/flight-data/json/2015-summary.json")

    flights.createOrReplaceTempView("flights")

    val just_usa_df = flights.where("dest_country_name = 'United States'")
    just_usa_df.selectExpr("*").explain


    // COMMAND ----------

    def power3(number:Double):Double = number * number * number
    spark.udf.register("power3", power3(_:Double):Double)


    // COMMAND ----------

    spark.sql(
      """
        SELECT count, power3(count) as power3_count FROM flights
      """.stripMargin).show()


    // COMMAND ----------
    val aaa = 1

  }

}

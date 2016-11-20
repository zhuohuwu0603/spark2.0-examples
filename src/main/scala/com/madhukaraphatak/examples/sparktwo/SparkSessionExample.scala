package com.madhukaraphatak.examples.sparktwo

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Spark Session example
  *
  */
object SparkSessionExample {

  val logger = Logger.getLogger(SparkSessionExample.getClass)

  def main(args: Array[String]) {

    logger.warn("Beginning of SparkSessionExample.")
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    val df = sparkSession.read.option("header","true").csv("src/main/resources/sales.csv")

    df.show()

  }

}

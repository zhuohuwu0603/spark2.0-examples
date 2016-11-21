package com.zhuohuawu.examples.sparktwo
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  val logger = Logger.getLogger(SparkSessionExample.getClass)
  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("spark-testing-example")
  val sc = new SparkContext(sparkConf)

  def main(args: Array[String]) {

    logger.warn("Beginning of App: " + this.getClass.getName)

    val countByWordRdd: RDD[(String, Int)] = sc.textFile("src/main/resources/intro.txt")
      .flatMap(l => l.split("\\W+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    countByWordRdd
      .foreach(println)
  }
}

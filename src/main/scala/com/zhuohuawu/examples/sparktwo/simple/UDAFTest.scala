package com.zhuohuawu.examples.sparktwo.simple

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kevinwu on 2017-07-28.
  */
object UDAFTest {
  def main(args: Array[String]) {
    //testHelloSpark
    testDataset
  }


  def testHelloSpark: Unit = {
    val logger = Logger.getLogger(UDAFTest.getClass)
    println("Hello, world!")

    val conf = new SparkConf().setAppName("HelloSpark").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    distData.foreach(println)
  }

  def testDataset(): Unit = {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    import sparkSession.implicits._
    val data = sparkSession.read.text("src/main/resources/data.txt").as[String]

    val words = data.flatMap(value => value.split("\\s+"))

    val groupedWords = words.groupByKey(_.toLowerCase)

    val counts = groupedWords.count()

    counts.show()
  }

  def testUDAF(): Unit = {

  }


}

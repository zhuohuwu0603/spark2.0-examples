package com.zhuohuawu.examples.sparktwo

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kevinwu on 2017-07-28.
  */
object HelloSpark {
  def main(args: Array[String]) {
    val logger = Logger.getLogger(HelloSpark.getClass)
    println("Hello, world!")

    val conf = new SparkConf().setAppName("HelloSpark").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    distData.foreach(println)
  }
}

package com.zhuohuawu.examples.sparktwo

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable

object MainStreaming {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("spark-streaming-testing-example")

  val ssc = new StreamingContext(sparkConf, Seconds(1))

  def main(args: Array[String]) {

    val rddQueue = new mutable.Queue[RDD[Char]]()

    ssc.queueStream(rddQueue)
      .map(_.toUpper)
      .window(windowDuration = Seconds(6), slideDuration = Seconds(2)) //6 means the window size, 2 means slide right with offset = 2
      .print()

    ssc.start()

    for (c <- 'a' to 'z') {
      rddQueue += ssc.sparkContext.parallelize(List(c))
    }

    ssc.awaitTermination()
  }
}

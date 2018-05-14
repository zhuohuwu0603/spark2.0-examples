package com.zhuohuawu.examples.sparktwo.simple

import org.apache.log4j.Logger
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
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

    val spark = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()



    class MyAvg extends UserDefinedAggregateFunction {
      override def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil)

      override def bufferSchema: StructType = StructType(
        StructField("count", LongType) ::
        StructField("sum", DoubleType) :: Nil
      )

      //return type
      override def dataType: DataType = DoubleType

      override def deterministic: Boolean = true

      override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0L
        buffer(1) = 0.0
      }

      override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getAs[Long](0) + 1
        buffer(1) = buffer.getAs[Double](1) + input.getAs[Double](0)
      }

      override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
        buffer1(1) = buffer1.getAs[Long](1) + buffer2.getAs[Long](1)
      }

      override def evaluate(buffer: Row): Any = {
        buffer.getDouble(1) / buffer.getLong(0)
      }
    }

    val myAvg = new MyAvg
    import spark.implicits._
    spark.udf.register("ourAvg", myAvg)

    import org.apache.spark.ml._

    val lr = new LogisticRegressionWithLBFGS()

  }


}

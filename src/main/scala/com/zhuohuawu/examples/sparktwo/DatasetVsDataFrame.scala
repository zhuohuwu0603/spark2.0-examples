package com.zhuohuawu.examples.sparktwo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
  * Logical Plans for Dataframe and Dataset
  */
object DatasetVsDataFrame {

  case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    import sparkSession.implicits._


    //read data from text file

//    val df = sparkSession.read.option("header","true").option("inferSchema","true").csv("src/main/resources/sales.csv")
//    val ds = sparkSession.read.option("header","true").option("inferSchema","true").csv("src/main/resources/sales.csv").as[Sales]
//
//
//    val selectedDF = df.select("itemId")
//
//    val selectedDS = ds.map(_.itemId)
//
//    println(selectedDF.queryExecution.optimizedPlan.numberedTreeString)
//
//    println(selectedDS.queryExecution.optimizedPlan.numberedTreeString)

    //val path = "src/main/resources/simple/sample.json"

    //val path = "src/main/resources/simple/a_struct.json"
    val path = "src/main/resources/simple/nested1.json"
    test2(sparkSession, path)

  }

  // https://databricks.com/blog/2017/02/23/working-complex-data-formats-structured-streaming-apache-spark-2-1.html
  // https://app.pluralsight.com/player?course=mongodb-introduction&author=nuri-halperin&name=mongodb-introduction-m2&clip=3&mode=live
  def test1(sparkSession: SparkSession, path: String): Unit = {

    println("Hello, test1")
    //val df = sparkSession.read.option("header","true").option("inferSchema","true").json("src/main/resources/simple/sample.json")
    //val df = sparkSession.read.option("header","true").option("inferSchema","true").json(path)
    val df = sparkSession.read.json(path)

    import org.apache.spark.sql.functions.explode
    val dfDates = df.select(explode(df("dates"))).toDF("dates")
    dfDates.show(false)


    val dfContent = df.select(explode(df("content"))).toDF("content")
    dfContent.show(false)

    val dfFooBar = dfContent.select("content.foo", "content.bar")
    dfFooBar.show(false)


    val aaa = 1

  }

  def test2(spark: SparkSession, path: String): Unit = {

    println("Hello, struct")
    //val df = sparkSession.read.option("header","true").option("inferSchema","true").json("src/main/resources/simple/sample.json")
    //val df = sparkSession.read.option("header","true").option("inferSchema","true").json(path)

//    val schema1 = new StructType().add("b", IntegerType)

    val schema1 = new StructType().add("a", new StructType().add("b", IntegerType))

    val df = spark.read
                  //.option("inferSchema","true")

                  .option("multipleline", true)
                  //.option("mode", "PERMISSIVE")
                  .schema(schema1)
                  .json(path)

//    import org.apache.spark.sql.functions.explode
//    val dfDates = df.select(explode(df("dates"))).toDF("dates")
//    dfDates.show(false)

    df.printSchema()

    df.show(false)

    val aab = 1

//    import spark.implicits._
//    import org.apache.spark.sql.functions._
//
//    df.select(from_json('a, schema1)).show(false)


//    import org.apache.spark.sql.DataFrame
//    import org.apache.spark.sql.functions._
//    import org.apache.spark.sql.types._
//
//    // Convenience function for turning JSON strings into DataFrames.
//    def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
//      // SparkSessions are available with Spark 2.0+
//      val reader = spark.read
//      //Option(schema).foreach(reader.schema)
//      reader.json(spark.sparkContext.parallelize(Array(json)))
//    }
//
//    val schema = new StructType().add("a", new StructType().add("b", IntegerType))
//
//    val events = jsonToDataFrame("""
//      {
//        "a": {
//           "b": 1
//        }
//      }
//      """, schema)
//
//    events.select("a.b").show(false)


//    import org.apache.spark.sql.types._                         // include the Spark Types to define our schema
//    import org.apache.spark.sql.functions._                     // include the Spark helper functions
//
//    import spark.implicits._
//
//    val jsonSchema = new StructType()
//      .add("battery_level", LongType)
//      .add("c02_level", LongType)
//      .add("cca3",StringType)
//      .add("cn", StringType)
//      .add("device_id", LongType)
//      .add("device_type", StringType)
//      .add("signal", LongType)
//      .add("ip", StringType)
//      .add("temp", LongType)
//      .add("timestamp", TimestampType)
//
//    // define a case class
//    case class DeviceData (id: Int, device: String)
//    // create some sample data
//    val eventsDS = Seq (
//      (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
//      (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""),
//      (2, """{"device_id": 2, "device_type": "sensor-ipad", "ip": "88.36.5.1", "cca3": "ITA", "cn": "Italy", "temp": 18, "signal": 25, "battery_level": 5, "c02_level": 1372, "timestamp" :1475600500 }"""),
//      (3, """{"device_id": 3, "device_type": "sensor-inest", "ip": "66.39.173.154", "cca3": "USA", "cn": "United States", "temp": 47, "signal": 12, "battery_level": 1, "c02_level": 1447, "timestamp" :1475600502 }"""),
//      (4, """{"device_id": 4, "device_type": "sensor-ipad", "ip": "203.82.41.9", "cca3": "PHL", "cn": "Philippines", "temp": 29, "signal": 11, "battery_level": 0, "c02_level": 983, "timestamp" :1475600504 }"""),
//      (5, """{"device_id": 5, "device_type": "sensor-istick", "ip": "204.116.105.67", "cca3": "USA", "cn": "United States", "temp": 50, "signal": 16, "battery_level": 8, "c02_level": 1574, "timestamp" :1475600506 }"""),
//      (6, """{"device_id": 6, "device_type": "sensor-ipad", "ip": "220.173.179.1", "cca3": "CHN", "cn": "China", "temp": 21, "signal": 18, "battery_level": 9, "c02_level": 1249, "timestamp" :1475600508 }"""),
//      (7, """{"device_id": 7, "device_type": "sensor-ipad", "ip": "118.23.68.227", "cca3": "JPN", "cn": "Japan", "temp": 27, "signal": 15, "battery_level": 0, "c02_level": 1531, "timestamp" :1475600512 }"""),
//      (8 ,""" {"device_id": 8, "device_type": "sensor-inest", "ip": "208.109.163.218", "cca3": "USA", "cn": "United States", "temp": 40, "signal": 16, "battery_level": 9, "c02_level": 1208, "timestamp" :1475600514 }"""),
//      (9,"""{"device_id": 9, "device_type": "sensor-ipad", "ip": "88.213.191.34", "cca3": "ITA", "cn": "Italy", "temp": 19, "signal": 11, "battery_level": 0, "c02_level": 1171, "timestamp" :1475600516 }"""),
//      (10,"""{"device_id": 10, "device_type": "sensor-igauge", "ip": "68.28.91.22", "cca3": "USA", "cn": "United States", "temp": 32, "signal": 26, "battery_level": 7, "c02_level": 886, "timestamp" :1475600518 }"""),
//      (11,"""{"device_id": 11, "device_type": "sensor-ipad", "ip": "59.144.114.250", "cca3": "IND", "cn": "India", "temp": 46, "signal": 25, "battery_level": 4, "c02_level": 863, "timestamp" :1475600520 }"""),
//      (12, """{"device_id": 12, "device_type": "sensor-igauge", "ip": "193.156.90.200", "cca3": "NOR", "cn": "Norway", "temp": 18, "signal": 26, "battery_level": 8, "c02_level": 1220, "timestamp" :1475600522 }"""),
//      (13, """{"device_id": 13, "device_type": "sensor-ipad", "ip": "67.185.72.1", "cca3": "USA", "cn": "United States", "temp": 34, "signal": 20, "battery_level": 8, "c02_level": 1504, "timestamp" :1475600524 }"""),
//      (14, """{"device_id": 14, "device_type": "sensor-inest", "ip": "68.85.85.106", "cca3": "USA", "cn": "United States", "temp": 39, "signal": 17, "battery_level": 8, "c02_level": 831, "timestamp" :1475600526 }"""),
//      (15, """{"device_id": 15, "device_type": "sensor-ipad", "ip": "161.188.212.254", "cca3": "USA", "cn": "United States", "temp": 27, "signal": 26, "battery_level": 5, "c02_level": 1378, "timestamp" :1475600528 }"""),
//      (16, """{"device_id": 16, "device_type": "sensor-igauge", "ip": "221.3.128.242", "cca3": "CHN", "cn": "China", "temp": 10, "signal": 24, "battery_level": 6, "c02_level": 1423, "timestamp" :1475600530 }"""),
//      (17, """{"device_id": 17, "device_type": "sensor-ipad", "ip": "64.124.180.215", "cca3": "USA", "cn": "United States", "temp": 38, "signal": 17, "battery_level": 9, "c02_level": 1304, "timestamp" :1475600532 }"""),
//      (18, """{"device_id": 18, "device_type": "sensor-igauge", "ip": "66.153.162.66", "cca3": "USA", "cn": "United States", "temp": 26, "signal": 10, "battery_level": 0, "c02_level": 902, "timestamp" :1475600534 }"""),
//      (19, """{"device_id": 19, "device_type": "sensor-ipad", "ip": "193.200.142.254", "cca3": "AUT", "cn": "Austria", "temp": 32, "signal": 27, "battery_level": 5, "c02_level": 1282, "timestamp" :1475600536 }""")).toDF("id", "device").as[DeviceData]
//
//
//
//    eventsDS.show(false)
//    eventsDS.printSchema()

    val aaa = 1

  }

}

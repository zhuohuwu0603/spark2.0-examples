package com.zhuohuawu.examples.sparktwo.nb2code

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Spark Session example
  *
  */

// https://community.cloud.databricks.com/?o=8920468172695095#notebook/3588089277658157/command/3588089277658225
// convert from 20160602_NewCircle_DF_DS notebook
// download source file: %sh wget http://media.mongodb.org/zips.json
object NB_20160602_NewCircle_Structural_Streaming {

  val logger = Logger.getLogger(NB_20160602_NewCircle_Structural_Streaming.getClass)

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger(NB_20160602_NewCircle_Structural_Streaming.getClass).setLevel(Level.INFO)

    logger.error("Beginning of NB_20160602_NewCircle_Structural_Streaming.")
    val spark = SparkSession.builder.
      master("local[4]")
      .appName("NewCircleDFDSExample example")
      .getOrCreate()


    import org.apache.spark.sql.types._

    val schema = StructType(StructField("lastname", StringType, false) :: StructField("email", StringType, false)
      :: StructField("hits", IntegerType, false) :: Nil)

    val streamInputPath = "src/main/resources/stream/in/"
    val streamOutputPath = "src/main/resources/stream/out/"
    val streamCheckpointPath = "src/main/resources/stream/ck/"

    import spark.implicits._
    val query = spark.readStream.schema(schema).json(streamInputPath)


    import org.apache.spark.sql.functions._
     // val modifiedData = query.select($"hits", $"lastname", current_timestamp() as "now", lower(query("email")) as "email")
//     val modifiedData = query.select($"lastname", lower(query("email")) as "email", current_timestamp() as "now", $"hits")
    val modifiedData = query.select("lastname")


    val stream = modifiedData.writeStream.option("checkpointLocation", streamCheckpointPath).start(streamOutputPath)
   // val df = spark.read.parquet(streamOutputPath)

    //df.select("email", "hits").groupBy("email").sum("hits").show(false)
    //df.show(false)
    stream.awaitTermination()


    println("hello, end")
    val abc = 1

  }

}

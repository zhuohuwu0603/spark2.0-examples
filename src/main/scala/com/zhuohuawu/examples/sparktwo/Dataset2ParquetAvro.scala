package com.zhuohuawu.examples.sparktwo

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Logical Plans for Dataframe and Dataset
  */
object Dataset2ParquetAvro {

  case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    import sparkSession.implicits._


    //read data from text file

    val df = sparkSession.read.option("header","true").option("inferSchema","true").csv("src/main/resources/sales.csv")
    val ds = sparkSession.read.option("header","true").option("inferSchema","true").csv("src/main/resources/sales.csv").as[Sales]

    //println(ds)
    ds.show(false)

    val parquetOutputPath = "src/main/output/parquet/"

    // write to parquet
    ds.coalesce(4).write.mode(SaveMode.Overwrite).parquet(parquetOutputPath)
    // read from parquet
    val dsParquet = sparkSession.read.parquet(parquetOutputPath)
    println("read from parquet")
    dsParquet.show(false)

    val avroOutputPath = "src/main/output/avro/"
    ds.coalesce(4).write.format("com.databricks.spark.avro").mode(SaveMode.Overwrite).save(avroOutputPath)
    val dsAvro = sparkSession.read.format("com.databricks.spark.avro").load(avroOutputPath)
    println("===================== read from avro ")
    dsAvro.show(false)

    val aaa = 1


//    val selectedDF = df.select("itemId")
//
//    val selectedDS = ds.map(_.itemId)
//
//    println(selectedDF.queryExecution.optimizedPlan.numberedTreeString)
//
//    println(selectedDS.queryExecution.optimizedPlan.numberedTreeString)
//

  }

}

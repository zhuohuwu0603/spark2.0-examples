package com.zhuohuawu.examples.sparktwo.definitiveguide

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Spark Session example
  *
  */
/** TODO ???

Error:(32, 31) Unable to find encoder for type stored in a Dataset.  Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases.
    val flights = flightsDF.as[Flight]

Error:(32, 31) not enough arguments for method as: (implicit evidence$2: org.apache.spark.sql.Encoder[Flight])org.apache.spark.sql.Dataset[Flight].
Unspecified value parameter evidence$2.
    val flights = flightsDF.as[Flight]

*/

object Ch03_01_Toolset {

  val logger = Logger.getLogger(Ch03_01_Toolset.getClass)

  def main(args: Array[String]) {

    logger.error("Beginning of SparkSessionExample.")

    val BASEPATH = "/Users/zhuohuawu/Documents/data/spark-definitive-guide"

    val spark = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    // in Scala
    import spark.implicits._
    case class Flight(DEST_COUNTRY_NAME: String,
                      ORIGIN_COUNTRY_NAME: String,
                      count: BigInt)
    val flightsDF = spark.read
      .parquet(BASEPATH + "/data/flight-data/parquet/2010-summary.parquet/")
    val flights = flightsDF.as[Flight]


    // COMMAND ----------

    // in Scala
    flights
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(flight_row => flight_row)
      .take(5)

    flights
      .take(5)
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))


    // COMMAND ----------

    // in Scala
    val staticDataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(BASEPATH + "/data/retail-data/by-day/*.csv")

    staticDataFrame.createOrReplaceTempView("retail_data")
    val staticSchema = staticDataFrame.schema


    // COMMAND ----------

    // in Scala
    import org.apache.spark.sql.functions.{window, column, desc, col}
    staticDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(
        col("CustomerId"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")
      .show(5)


    // COMMAND ----------

    spark.conf.set("spark.sql.shuffle.partitions", "5")


    // COMMAND ----------

    val streamingDataFrame = spark.readStream
      .schema(staticSchema)
      .option("maxFilesPerTrigger", 1)
      .format("csv")
      .option("header", "true")
      .load(BASEPATH + "/data/retail-data/by-day/*.csv")


    // COMMAND ----------

    streamingDataFrame.isStreaming // returns true


    // COMMAND ----------

    // in Scala
    val purchaseByCustomerPerHour = streamingDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(
        $"CustomerId", window($"InvoiceDate", "1 day"))
      .sum("total_cost")


    // COMMAND ----------

    // in Scala
    purchaseByCustomerPerHour.writeStream
      .format("memory") // memory = store in-memory table
      .queryName("customer_purchases") // the name of the in-memory table
      .outputMode("complete") // complete = all the counts should be in the table
      .start()


    // COMMAND ----------

    // in Scala
    spark.sql("""
  SELECT *
  FROM customer_purchases
  ORDER BY `sum(total_cost)` DESC
  """)
      .show(5)


    // COMMAND ----------

    staticDataFrame.printSchema()


    // COMMAND ----------

    // in Scala
    import org.apache.spark.sql.functions.date_format
    val preppedDataFrame = staticDataFrame
      .na.fill(0)
      .withColumn("day_of_week", date_format($"InvoiceDate", "EEEE"))
      .coalesce(5)


    // COMMAND ----------

    // in Scala
    val trainDataFrame = preppedDataFrame
      .where("InvoiceDate < '2011-07-01'")
    val testDataFrame = preppedDataFrame
      .where("InvoiceDate >= '2011-07-01'")


    // COMMAND ----------

    trainDataFrame.count()
    testDataFrame.count()


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.feature.StringIndexer
    val indexer = new StringIndexer()
      .setInputCol("day_of_week")
      .setOutputCol("day_of_week_index")


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.feature.OneHotEncoder
    val encoder = new OneHotEncoder()
      .setInputCol("day_of_week_index")
      .setOutputCol("day_of_week_encoded")


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.feature.VectorAssembler

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
      .setOutputCol("features")


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.Pipeline

    val transformationPipeline = new Pipeline()
      .setStages(Array(indexer, encoder, vectorAssembler))


    // COMMAND ----------

    // in Scala
    val fittedPipeline = transformationPipeline.fit(trainDataFrame)


    // COMMAND ----------

    // in Scala
    val transformedTraining = fittedPipeline.transform(trainDataFrame)


    // COMMAND ----------

    transformedTraining.cache()


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.clustering.KMeans
    val kmeans = new KMeans()
      .setK(20)
      .setSeed(1L)


    // COMMAND ----------

    // in Scala
    val kmModel = kmeans.fit(transformedTraining)


    // COMMAND ----------

    kmModel.computeCost(transformedTraining)


    // COMMAND ----------

    // in Scala
    val transformedTest = fittedPipeline.transform(testDataFrame)


    // COMMAND ----------

    kmModel.computeCost(transformedTest)


    // COMMAND ----------

    // in Scala
    spark.sparkContext.parallelize(Seq(1, 2, 3)).toDF()


    // COMMAND ----------



    val aaa = 1

  }

}

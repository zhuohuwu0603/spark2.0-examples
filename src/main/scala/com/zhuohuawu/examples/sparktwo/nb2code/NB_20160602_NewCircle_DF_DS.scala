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
object NB_20160602_NewCircle_DF_DS {

  val logger = Logger.getLogger(NB_20160602_NewCircle_DF_DS.getClass)

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger(NB_20160602_NewCircle_DF_DS.getClass).setLevel(Level.INFO)

    logger.warn("Beginning of NewCircleDFDSExample.")
    val spark = SparkSession.builder.
      master("local[4]")
      .appName("NewCircleDFDSExample example")
      .getOrCreate()


     // val df = spark.read.option("header","true").csv("src/main/resources/sales.csv")
     // df.show()

    val zipInputPath = "src/main/resources/zips.json"
    val df = spark.read.json(zipInputPath).createOrReplaceTempView("zip")

    spark.sql("DESCRIBE zip").show()
    spark.table("zip").withColumnRenamed("_id", "zip").createOrReplaceTempView("zip")
    spark.sql("DESCRIBE zip").show()

    //val top10PopInIL = "select COUNT(zip), sum(pop) as population, city from zip where state = 'IL' group by city order by sum(pop) desc limit 10"
    val top10PopInIL = """select COUNT(zip), sum(pop) as population, city from zip where state = 'IL' group by city order by sum(pop) desc limit 10""";
    spark.sql(top10PopInIL).show()

    val zipOutputPath = "src/main/resources/zip.csv";
    spark.sqlContext.table("zip").select("zip", "city", "state", "pop").write.format("csv").mode("overwrite").save(zipOutputPath)


    spark.sparkContext.textFile(zipOutputPath).take(3)

    val cacheTableString = """CACHE TABLE zip"""
    spark.sql(cacheTableString)


    import org.apache.spark.sql._

    println(classOf[DataFrame] == classOf[Dataset[_]])


    /////////// dataset

    val ds = spark.sqlContext.range(3)
    ds.printSchema
    // change type long to String

    //ds.as[String]
    //ds.printSchema

    // convert from ds to df
    ds.toDF.printSchema

    // language neurtral, create a table in scala and use it in python or any other languages
    val numbersTable = "numbers"
    ds.createOrReplaceTempView(numbersTable)
    spark.sqlContext.table(numbersTable).collect()




    // case class Zip need to be defined outside main class:
    //https://stackoverflow.com/questions/38664972/why-is-unable-to-find-encoder-for-type-stored-in-a-dataset-when-creating-a-dat
    import spark.implicits._
    spark.sqlContext.table("zip").as[Zip].show(3, false)

    val ds2 = spark.sqlContext.table("zip").as[Zip]
    println("loc = " + ds2.first.loc)

    //ds.groupBy("state").count.orderBy("state").show
    import org.apache.spark.sql._
    ds2.printSchema()
    ds2.groupBy(ds2("state")).count.orderBy(ds2("state")).show


    // complex
    spark.sqlContext.read.json(zipInputPath).withColumnRenamed("_id", "zip").as[Zip].filter(_.nearChicago).sort($"pop".desc).show(false)


    //query optimization: catalyst
    import org.apache.spark.sql.functions._

    // ds2.filter('pop > 10000).select(lower(ds2("city"))).filter(ds2("state") === "RI").show(5, false)
    ds2.filter('pop > 10000).select(lower('city)).filter('state === "RI").show(5, false)
    ds2.filter('pop > 10000).select(lower('city)).filter('state === "RI").explain(true)

    // catalyst explain
    spark.sqlContext.read.json(zipInputPath).withColumnRenamed("_id", "zip").as[Zip].filter(_.nearChicago).sort($"pop".desc).explain(true)


    //whote-stage codegen debug
    import org.apache.spark.sql.execution.debug._
    spark.sqlContext.read.json(zipInputPath).withColumnRenamed("_id", "zip").as[Zip].filter(_.nearChicago).debugCodegen


    spark.sqlContext.table("zip").write.mode("overwrite").saveAsTable("hive_zip")

    // save the database to : file:/Users/kevinwu/Documents/zw_codes/GitLab/spark2_codes/spark2.0-examples/spark-warehouse/
    spark.catalog.listDatabases.show(false)
    spark.catalog.listTables().show(false)

    println("hello, end")

    val abc = 1

  }

}



// the case class needs to be outside the main class scope??? more complicated type,
case class Zip(zip: String, city: String, loc: Array[Double], pop: Long, state: String) {
  val latChicago = 41.8781136
  val lonChicago = -87.6297982

  def nearChicago = {
    math.sqrt(math.pow(loc(0) - lonChicago, 2) + math.pow(loc(1) - latChicago, 2)) < 1
  }
}
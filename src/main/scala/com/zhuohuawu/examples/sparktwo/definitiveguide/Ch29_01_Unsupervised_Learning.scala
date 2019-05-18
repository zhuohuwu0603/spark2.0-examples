package com.zhuohuawu.examples.sparktwo.definitiveguide

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Spark Session example
  *
  */

object Ch29_01_Unsupervised_Learning {

  val logger = Logger.getLogger(Ch29_01_Unsupervised_Learning.getClass)

  def main(args: Array[String]) {

    logger.error("Beginning of SparkSessionExample.")

    val BASEPATH = "/Users/zhuohuawu/Documents/data/spark-definitive-guide"
    val BASE_OUTPUT_PATH = "/Users/zhuohuawu/Documents/zw_codes/GitLab/spark2_codes/spark2.0-examples/output/definitiveguide"

    val spark = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    // in Scala
    import org.apache.spark.ml.feature.VectorAssembler

    val va = new VectorAssembler()
      .setInputCols(Array("Quantity", "UnitPrice"))
      .setOutputCol("features")

    val sales = va.transform(spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(BASEPATH + "/data/retail-data/by-day/*.csv")
      .limit(50)
      .coalesce(1)
      .where("Description IS NOT NULL"))

    sales.cache()


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.clustering.KMeans
    val km = new KMeans().setK(5)
    println(km.explainParams())
    val kmModel = km.fit(sales)


    // COMMAND ----------

    // in Scala
    val summary1 = kmModel.summary
    summary1.clusterSizes // number of points
    kmModel.computeCost(sales)
    println("Cluster Centers: ")
    kmModel.clusterCenters.foreach(println)


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.clustering.BisectingKMeans
    val bkm = new BisectingKMeans().setK(5).setMaxIter(5)
    println(bkm.explainParams())
    val bkmModel = bkm.fit(sales)


    // COMMAND ----------

    // in Scala
    val summary = bkmModel.summary
    summary.clusterSizes // number of points
    kmModel.computeCost(sales)
    println("Cluster Centers: ")
    kmModel.clusterCenters.foreach(println)


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.clustering.GaussianMixture
    val gmm = new GaussianMixture().setK(5)
    println(gmm.explainParams())
    val gmmModel = gmm.fit(sales)


    // COMMAND ----------

    // in Scala
    val gmmSummary = gmmModel.summary
    gmmModel.weights
    gmmModel.gaussiansDF.show()
    gmmSummary.cluster.show()
    gmmSummary.clusterSizes
    gmmSummary.probability.show()


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.feature.{Tokenizer, CountVectorizer}
    val tkn = new Tokenizer().setInputCol("Description").setOutputCol("DescOut")
    val tokenized = tkn.transform(sales.drop("features"))
    val cv = new CountVectorizer()
      .setInputCol("DescOut")
      .setOutputCol("features")
      .setVocabSize(500)
      .setMinTF(0)
      .setMinDF(0)
      .setBinary(true)
    val cvFitted = cv.fit(tokenized)
    val prepped = cvFitted.transform(tokenized)


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.clustering.LDA
    val lda = new LDA().setK(10).setMaxIter(5)
    println(lda.explainParams())
    val model = lda.fit(prepped)


    // COMMAND ----------

    // in Scala
    model.describeTopics(3).show()
    cvFitted.vocabulary


    // COMMAND ----------



  }

}

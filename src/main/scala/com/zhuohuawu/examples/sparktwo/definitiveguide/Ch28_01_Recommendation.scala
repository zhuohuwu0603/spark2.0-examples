package com.zhuohuawu.examples.sparktwo.definitiveguide

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Spark Session example
  *
  */

object Ch28_01_Recommendation {

  val logger = Logger.getLogger(Ch28_01_Recommendation.getClass)

  def main(args: Array[String]) {

    logger.error("Beginning of SparkSessionExample.")

    val BASEPATH = "/Users/zhuohuawu/Documents/data/spark-definitive-guide"
    val BASE_OUTPUT_PATH = "/Users/zhuohuawu/Documents/zw_codes/GitLab/spark2_codes/spark2.0-examples/output/definitiveguide"

    val spark = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    import spark.implicits._

    // in Scala
    import org.apache.spark.ml.recommendation.ALS
    val ratings = spark.read.textFile(BASEPATH + "/data/sample_movielens_ratings.txt")
      .selectExpr("split(value , '::') as col")
      .selectExpr(
        "cast(col[0] as int) as userId",
        "cast(col[1] as int) as movieId",
        "cast(col[2] as float) as rating",
        "cast(col[3] as long) as timestamp")
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    println(als.explainParams())
    val alsModel = als.fit(training)
    val predictions = alsModel.transform(test)


    // COMMAND ----------

    // in Scala
    alsModel.recommendForAllUsers(10)
      .selectExpr("userId", "explode(recommendations)").show()
    alsModel.recommendForAllItems(10)
      .selectExpr("movieId", "explode(recommendations)").show()


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")


    // COMMAND ----------

    // in Scala
    import org.apache.spark.mllib.evaluation.{
      RankingMetrics,
      RegressionMetrics}
    val regComparison = predictions.select("rating", "prediction")
      .rdd.map(x => (x.getFloat(0).toDouble,x.getFloat(1).toDouble))
    val metrics = new RegressionMetrics(regComparison)


    // COMMAND ----------

    // in Scala
    import org.apache.spark.mllib.evaluation.{RankingMetrics, RegressionMetrics}
    import org.apache.spark.sql.functions.{col, expr}
    val perUserActual = predictions
      .where("rating > 2.5")
      .groupBy("userId")
      .agg(expr("collect_set(movieId) as movies"))


    // COMMAND ----------

    // in Scala
    val perUserPredictions = predictions
      .orderBy(col("userId"), col("prediction").desc)
      .groupBy("userId")
      .agg(expr("collect_list(movieId) as movies"))


    // COMMAND ----------

    // in Scala
    val perUserActualvPred = perUserActual.join(perUserPredictions, Seq("userId"))
      .map(row => (
        row(1).asInstanceOf[Seq[Integer]].toArray,
        row(2).asInstanceOf[Seq[Integer]].toArray.take(15)
      ))
    val ranks = new RankingMetrics(perUserActualvPred.rdd)


    // COMMAND ----------

    // in Scala
    ranks.meanAveragePrecision
    ranks.precisionAt(5)


    // COMMAND ----------



    // COMMAND ----------
    val aaa = 1

  }

}

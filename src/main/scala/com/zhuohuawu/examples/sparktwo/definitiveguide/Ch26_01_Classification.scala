package com.zhuohuawu.examples.sparktwo.definitiveguide

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Spark Session example
  *
  */

object Ch26_01_Classification {

  val logger = Logger.getLogger(Ch26_01_Classification.getClass)

  def main(args: Array[String]) {

    logger.error("Beginning of SparkSessionExample.")

    val BASEPATH = "/Users/zhuohuawu/Documents/data/spark-definitive-guide"
    val BASE_OUTPUT_PATH = "/Users/zhuohuawu/Documents/zw_codes/GitLab/spark2_codes/spark2.0-examples/output/definitiveguide"

    val spark = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    // in Scala
    val bInput = spark.read.format("parquet").load(BASEPATH + "/data/binary-classification")
      .selectExpr("features", "cast(label as double) as label")


    // COMMAND ----------

    import spark.implicits._

    // in Scala
    import org.apache.spark.ml.classification.LogisticRegression
    val lr = new LogisticRegression()
    println(lr.explainParams()) // see all parameters
    val lrModel = lr.fit(bInput)


    // COMMAND ----------

    // in Scala
    println(lrModel.coefficients)
    println(lrModel.intercept)


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary
    val summary = lrModel.summary
    val bSummary = summary.asInstanceOf[BinaryLogisticRegressionSummary]
    println(bSummary.areaUnderROC)
    bSummary.roc.show()
    bSummary.pr.show()


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.classification.DecisionTreeClassifier
    val dt = new DecisionTreeClassifier()
    println(dt.explainParams())
    val dtModel = dt.fit(bInput)


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.classification.RandomForestClassifier
    val rfClassifier = new RandomForestClassifier()
    println(rfClassifier.explainParams())
    val rfModel = rfClassifier.fit(bInput)


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.classification.GBTClassifier
    val gbtClassifier = new GBTClassifier()
    println(gbtClassifier.explainParams())
    val gbtModel = gbtClassifier.fit(bInput)


    // COMMAND ----------

    // in Scala
    import org.apache.spark.ml.classification.NaiveBayes
    val nb = new NaiveBayes()
    println(nb.explainParams())
    val nbModel = nb.fit(bInput.where("label != 0"))


    // COMMAND ----------

    // in Scala
    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    val out = gbtModel.transform(bInput)
      .select("prediction", "label")
      .rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
    val metrics = new BinaryClassificationMetrics(out)


    // COMMAND ----------

    // in Scala
    metrics.areaUnderPR
    metrics.areaUnderROC
    println("Receiver Operating Characteristic")
    metrics.roc.toDF().show()


    // COMMAND ----------



    // COMMAND ----------
    val aaa = 1

  }

}

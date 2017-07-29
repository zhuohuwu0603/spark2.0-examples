package com.zhuohuawu.examples.sparktwo.nb2code

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Model, Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Session example
  *
  */

// https://community.cloud.databricks.com/?o=8920468172695095#notebook/1809612576125024/command/1809612576125084
// convert from NB_20160606_Power_Plant_ML_Pipeline
// download source file: https://archive.ics.uci.edu/ml/datasets/Combined+Cycle+Power+Plant
object NB_20160606_Power_Plant_ML_Pipeline {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger(NB_20160606_Power_Plant_ML_Pipeline.getClass).setLevel(Level.ERROR)
  val logger = Logger.getLogger(NB_20160606_Power_Plant_ML_Pipeline.getClass)

//  val spark = getSparkSession()
//  val sc = spark.sparkContext
//  val sqlContext = spark.sqlContext
//  val ssc = new StreamingContext(sc, Seconds(2))

  def main2(args: Array[String]): Unit = {

    logger.error("Beginning of NB_20160606_Power_Plant_ML_Pipeline.")

    val spark = getSparkSession()

    val sqlDeletePowerPlantPredictions = "DROP TABLE IF EXISTS power_plant_predictions"

    val sqlCreatePowerPlantPredictions = """CREATE TABLE power_plant_predictions(
      AT Double,
      V Double,
      AP Double,
      RH Double,
      PE Double,
      Predicted_PE Double
    )
    """

    println("sqlCreatePowerPlantPredictions is executed: ")
    spark.sql(sqlDeletePowerPlantPredictions)
    spark.sql(sqlCreatePowerPlantPredictions)
  }

  def getSparkSession(): SparkSession = {
    val spark = SparkSession.builder.
      master("local[4]")
      .appName("NB_20160606_Power_Plant_ML_Pipeline example")
      .getOrCreate()
    return spark
  }

  def main(args: Array[String]) {

    logger.error("Beginning of NB_20160606_Power_Plant_ML_Pipeline.")

    val spark = getSparkSession()
    val inputPath = "src/main/resources/power-plant/data/"
    val rawTextDF = readInputData(spark, inputPath)

    val (vectorizer: VectorAssembler, dataset: Dataset[Row], testSet: Dataset[Row], trainingSet: Dataset[Row]) = trainAndTestDataPrepration(spark)
    val crossval = new CrossValidator()

    val (rmse2, explainedVariance2, r2_2) = linearRegressionModel(spark, vectorizer, trainingSet, testSet, crossval, dataset)

    val (rmse3, explainedVariance3, r2_3) = decisionTreeModel(vectorizer, trainingSet, testSet, crossval)

//     val (rmse4, explainedVariance4, r2_4, finalModel) = gbtModel(spark, vectorizer, trainingSet, testSet, crossval)
    val finalModel = gbtModel(spark, vectorizer, trainingSet, testSet, crossval)

    println(f"Root Mean Squared Error2:  $rmse2")
    println(f"Explained Variance2: $explainedVariance2")
    println(f"R2_2: $r2_2")

    println(f"Root Mean Squared Error3: $rmse3")
    println(f"Explained Variance3: $explainedVariance3")
    println(f"R2_3: $r2_3")

    sparkStreamingPowerPlant(spark, finalModel)

    println("hello, end3")
  }

  /**
    * read data from raw input path into a dataframe
    * @param spark
    * @param inputPath
    * @return
    */
  def readInputData(spark: SparkSession, inputPath: String) = {
    println("Begin to readInputData()")
    //val inputPath = "src/main/resources/power-plant/data/"

    val rawTextDF = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath)
    rawTextDF.take(5).foreach(println)

    val powerPlantDF = rawTextDF

    powerPlantDF.createOrReplaceTempView("power_plant")

//    spark.sql("SELECT * FROM power_plant").show(false)
//    spark.sql("desc power_plant").show(false)

    spark.sqlContext.table("power_plant").describe().show(false)

    // ANSWER: Do a scatter plot of Power(PE) as a function of Temperature (AT).
    // BONUS: Name the y-axis "Power" and the x-axis "Temperature"

//    spark.sql("select AT as Temperature, PE as Power from power_plant").show(false)
//    spark.sql("select V as ExhaustVacuum, PE as Power from power_plant").show(false)
//    spark.sql("select RH Humidity, PE Power from power_plant").show(false)

    println("End to readInputData()")
    rawTextDF
  }

  /**
    * split data into train and test
    * @param spark
    * @return
    */
  private def trainAndTestDataPrepration(spark: SparkSession) = {
    // Step 5: Data Preparation

    println("Begin to trainAndTestDataPrepration()")

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val ssc = new StreamingContext(sc, Seconds(2))

    import org.apache.spark.ml.feature.VectorAssembler
    val dataset = sqlContext.table("power_plant")
    val vectorizer = new VectorAssembler()
    vectorizer.setInputCols(Array("AT", "V", "AP", "RH"))
    vectorizer.setOutputCol("features")

    // Step 6: Data Modeling
    var Array(split20, split80) = dataset.randomSplit(Array(0.20, 0.80), 1800009193L)
    val testSet = split20.cache()
    val trainingSet = split80.cache()
    println("End to trainAndTestDataPrepration()")

    (vectorizer, dataset, testSet, trainingSet)
  }

  /**
    * Linear regression model:
    *
    * @param spark
    * @param vectorizer
    * @param trainingSet
    * @param testSet
    * @param crossval
    * @param dateset
    */
  private def linearRegressionModel(spark: SparkSession, vectorizer: VectorAssembler, trainingSet: Dataset[Row], testSet: Dataset[Row], crossval: CrossValidator, dateset: Dataset[Row]) = {

    println("Begin to linearRegressionModel()")

    val sc = spark.sparkContext
    //val sqlContext = spark.sqlContext
    //val ssc = new StreamingContext(sc, Seconds(2))

    // ***** LINEAR REGRESSION MODEL ****

    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}

    // Let's initialize our linear regression learner
    val lr = new LinearRegression()
    lr.explainParams()
    // Now we set the parameters for the method
    lr.setPredictionCol("Predicted_PE")
      .setLabelCol("PE")
      .setMaxIter(100)

    // We will use the new spark.ml pipeline API. If you have worked with scikit-learn this will be very familiar.
    val lrPipeline = new Pipeline()
    lrPipeline.setStages(Array(vectorizer, lr))

    // Let's first train on the entire dataset to see what we get
    val lrModel = lrPipeline.fit(trainingSet)

    def toEquation(model: PipelineModel, dataset: Dataset[Row]): String = {
      // The intercept is as follows:
      val intercept = lrModel.stages(1).asInstanceOf[LinearRegressionModel].intercept

      // The coefficents (i.e. weights) are as follows:
      val weights = lrModel.stages(1).asInstanceOf[LinearRegressionModel].coefficients.toArray

      val featuresNoLabel = dataset.columns.filter(col => col != "PE")
      val coefficents = sc.parallelize(weights).zip(sc.parallelize(featuresNoLabel))
      var equation = s"y = $intercept "
      var variables = Array

      // Now let's sort the coeffecients from the most to the least and append them to the equation.
      coefficents.sortByKey().collect().foreach(x => {
          val weight = Math.abs(x._1)
          val name = x._2
          val symbol = if (x._1 > 0) "+" else "-"
          equation += (s" $symbol (${weight} * ${name})")
        }
      )

      // Finally here is our equation
      equation
    }

    println("Linear Regression Equation: " + toEquation(lrModel, dateset))

    val predictionsAndLabels = lrModel.transform(testSet)
    predictionsAndLabels.select("AT", "V", "AP", "RH", "PE", "Predicted_PE").show(false)

    //Now let's compute some evaluation metrics against our test dataset
    import org.apache.spark.mllib.evaluation.RegressionMetrics
    val metrics = new RegressionMetrics(predictionsAndLabels.select("Predicted_PE", "PE").rdd.map(r => (r(0).asInstanceOf[Double], r(1).asInstanceOf[Double])))

    val rmse1 = metrics.rootMeanSquaredError
    val explainedVariance1 = metrics.explainedVariance
    val r2_1 = metrics.r2

    println(f"Root Mean Squared Error1: $rmse1")
    println(f"Explained Variance1: $explainedVariance1")
    println(f"R2_1: $r2_1")

    println("End to linearRegressionModel()")

    // First we calculate the residual error and divide it by the RMSE
    predictionsAndLabels.selectExpr("PE", "Predicted_PE", "PE - Predicted_PE Residual_Error", s""" abs(PE - Predicted_PE) / $rmse1 Within_RSME""").createOrReplaceTempView("Power_Plant_RMSE_Evaluation")

    // First we calculate the residual error and divide it by the RMSE

//    spark.sql("SELECT * from Power_Plant_RMSE_Evaluation").show(false)

    // Now we can display the RMSE as a Histogram. Clearly this shows that the RMSE is centered around 0 with the vast majority of the error within 2 RMSEs.
//    spark.sql("SELECT Within_RSME  from Power_Plant_RMSE_Evaluation").show(false)

//    val sqlQuery = "SELECT case when Within_RSME <= 1.0 and Within_RSME >= -1.0 then 1  when  Within_RSME <= 2.0 and Within_RSME >= -2.0 then 2 else 3 end RSME_Multiple, COUNT(*) count  from Power_Plant_RMSE_Evaluation\ngroup by case when Within_RSME <= 1.0 and Within_RSME >= -1.0 then 1  when  Within_RSME <= 2.0 and Within_RSME >= -2.0 then 2 else 3 end"
//    spark.sql(sqlQuery).show(false)

    println("Begin to fine tune linearRegressionModel()")

    import org.apache.spark.ml.evaluation._
    import org.apache.spark.ml.tuning.ParamGridBuilder
    //first let's use a cross validator to split the data into training and validation subsets

    //Let's set up our evaluator class to judge the model based on the best root mean squared error
    val regEval = new RegressionEvaluator()
    regEval.setLabelCol("PE")
      .setPredictionCol("Predicted_PE")
      .setMetricName("rmse")

    //Let's create our crossvalidator with 5 fold cross validation
//    val crossval = new CrossValidator()
    crossval.setEstimator(lrPipeline)
    crossval.setNumFolds(5)
    crossval.setEvaluator(regEval)

    // Step 7: Tuning and Evaluation

    //Let's tune over our regularization parameter from 0.01 to 0.10
    val regParam = ((1 to 10) toArray).map(x => (x /100.0))

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, regParam)
      .build()
    crossval.setEstimatorParamMaps(paramGrid)

    //Now let's create our model
    val cvModel = crossval.fit(trainingSet)


    val predictionsAndLabels2 = cvModel.transform(testSet)
    val metrics2 = new RegressionMetrics(predictionsAndLabels2.select("Predicted_PE", "PE").rdd.map(r => (r(0).asInstanceOf[Double], r(1).asInstanceOf[Double])))

    val rmse2 = metrics2.rootMeanSquaredError
    val explainedVariance2 = metrics2.explainedVariance
    val r2_2 = metrics2.r2

    println(f"Root Mean Squared Error2: $rmse2")
    println(f"Explained Variance2: $explainedVariance2")
    println(f"R2_2: $r2_2")

    println("End to fine tune linearRegressionModel()")
    (rmse2, explainedVariance2, r2_2)

  }

  /**
    * create a decision tree model
    *
    * @param vectorizer
    * @param trainingSet
    * @param testSet
    * @param crossval
    * @return
    */
  private def decisionTreeModel(vectorizer: VectorAssembler, trainingSet: Dataset[Row], testSet: Dataset[Row], crossval: CrossValidator) = {
    // Decision tree:

    println("Begin to decisionTreeModel()")

    import org.apache.spark.ml.regression.DecisionTreeRegressor

    val dt = new DecisionTreeRegressor()
    dt.setLabelCol("PE")
    dt.setPredictionCol("Predicted_PE")
    dt.setFeaturesCol("features")
    // dt.setMaxBins(100)
    dt.setMaxBins(10)

    val dtPipeline = new Pipeline()
    dtPipeline.setStages(Array(vectorizer, dt))
    //Let's just resuse our CrossValidator

    crossval.setEstimator(dtPipeline)

    val paramGrid3 = new ParamGridBuilder()
      .addGrid(dt.maxDepth, Array(2, 3))
      .build()
    crossval.setEstimatorParamMaps(paramGrid3)

    val dtModel = crossval.fit(trainingSet)

    import org.apache.spark.ml.PipelineModel
    import org.apache.spark.ml.regression.DecisionTreeRegressionModel


    val predictionsAndLabels3 = dtModel.bestModel.transform(testSet)
    val metrics3 = new RegressionMetrics(predictionsAndLabels3.select("Predicted_PE", "PE").rdd.map(r => (r(0).asInstanceOf[Double], r(1).asInstanceOf[Double])))

    val rmse3 = metrics3.rootMeanSquaredError
    val explainedVariance3 = metrics3.explainedVariance
    val r2_3 = metrics3.r2

    println(f"Root Mean Squared Error3: $rmse3")
    println(f"Explained Variance3: $explainedVariance3")
    println(f"R2_3: $r2_3")

    // This line will pull the Decision Tree model from the Pipeline as display it as an if-then-else string
    dtModel.bestModel.asInstanceOf[PipelineModel].stages.last.asInstanceOf[DecisionTreeRegressionModel].toDebugString

    println("End to decisionTreeModel()")
    (rmse3, explainedVariance3, r2_3)
  }

  /**
    * create a Gradient-Boosted Decision Tree
    *
    * @param spark
    * @param vectorizer
    * @param trainingSet
    * @param testSet
    * @param crossval
    * @return
    */
  private def gbtModel(spark: SparkSession, vectorizer: VectorAssembler, trainingSet: Dataset[Row], testSet: Dataset[Row], crossval: CrossValidator) = {
    // GBT
    println("Begin to gbtModel()")

    import org.apache.spark.ml.regression.GBTRegressor

    val gbt = new GBTRegressor()
    gbt.setLabelCol("PE")
    gbt.setPredictionCol("Predicted_PE")
    gbt.setFeaturesCol("features")
    gbt.setSeed(100088121L)
    //gbt.setMaxBins(100)
    //gbt.setMaxIter(120)
    gbt.setMaxBins(10)
    gbt.setMaxIter(10)

    val gbtPipeline = new Pipeline()
    gbtPipeline.setStages(Array(vectorizer, gbt))
    //Let's just resuse our CrossValidator

    crossval.setEstimator(gbtPipeline)

    val paramGrid4 = new ParamGridBuilder()
      .addGrid(gbt.maxDepth, Array(2, 3))
      .build()
    crossval.setEstimatorParamMaps(paramGrid4)

    //gbt.explainParams
    val gbtModel = crossval.fit(trainingSet)

    import org.apache.spark.ml.regression.GBTRegressionModel

    val predictionsAndLabels4 = gbtModel.bestModel.transform(testSet)
    val metrics4 = new RegressionMetrics(predictionsAndLabels4.select("Predicted_PE", "PE").rdd.map(r => (r(0).asInstanceOf[Double], r(1).asInstanceOf[Double])))

    val rmse4 = metrics4.rootMeanSquaredError
    val explainedVariance4 = metrics4.explainedVariance
    val r2_4 = metrics4.r2
    gbtModel.bestModel.asInstanceOf[PipelineModel].stages.last.asInstanceOf[GBTRegressionModel].toDebugString

    println(f"Root Mean Squared Error4: $rmse4")
    println(f"Explained Variance4: $explainedVariance4")
    println(f"R2_4: $r2_4")

    println("End to gbtModel()")

    // Let's set the variable finalModel to our best GBT Model
    val finalModel = gbtModel.bestModel

    val sqlDeletePowerPlantPredictions = "DROP TABLE IF EXISTS power_plant_predictions"
    val sqlCreatePowerPlantPredictions =
      """CREATE TABLE power_plant_predictions(
      AT Double,
      V Double,
      AP Double,
      RH Double,
      PE Double,
      Predicted_PE Double
    )
    """

    println("sqlCreatePowerPlantPredictions is executed: ")
//    spark.sql(sqlDeletePowerPlantPredictions)
//    spark.sql(sqlCreatePowerPlantPredictions)

    // can not return multiple value, do not conform to class Model's type parameter bounds [M <: org.apache.spark.ml.Model[M]]
    //  (rmse4, explainedVariance4, r2_4, finalModel)
    finalModel
  }

  /**
    * create a spark streaming pipeline
    *
    * @param spark
    * @param finalModel
    */
  def sparkStreamingPowerPlant(spark: SparkSession, finalModel: Model[_]) = {
    // Streaming

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    //val ssc = new StreamingContext(sc, Seconds(2))

    // import org.apache.spark.Logging
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    //import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
    //import org.apache.log4j.Logger
    //import org.apache.log4j.Level
    import sqlContext._
    //import net.liftweb.json.DefaultFormats
    //import net.liftweb.json._

    import scala.collection.mutable.SynchronizedQueue


    val queue = new SynchronizedQueue[RDD[String]]()

    queue += sc.makeRDD(Seq(s"""{"AT":1.11,"V":22.2,"AP":3333.33,"RH":44.44,"PE":555.5}"""))

    val batchIntervalSeconds = 2

    var newContextCreated = false // Flag to detect whether new context was created or not

    // Function to create a new StreamingContext and set it up
    def creatingFunc(): StreamingContext = {

      // Create a StreamingContext
      val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))
      val batchInterval = Seconds(1)
      ssc.remember(Seconds(300))
      val dstream = ssc.queueStream(queue)
      dstream.foreachRDD {
        rdd =>
          if (!(rdd.isEmpty())) {
            println("rdd is not empty!")
            rdd.take(3).foreach(println)
            finalModel.transform(read.json(rdd).toDF()).write.mode(SaveMode.Overwrite).saveAsTable("power_plant_predictions")
          } else {
            println("rdd is empty!")
          }
      }
      println("Creating function called to create new StreamingContext for Power Plant Predictions")
      newContextCreated = true
      ssc
    }

    val ssc = StreamingContext.getActiveOrCreate(creatingFunc)
    if (newContextCreated) {
      println("New context created from currently defined creating function")
    } else {
      println("Existing context running or recovered from checkpoint, may not be running currently defined creating function")
    }

    //   // spark.sql("truncate table power_plant_predictions")
    //    // First we try it with a record from our test set and see what we get:
    //    queue += sc.makeRDD(Seq(s"""{"AT":10.82,"V":37.5,"AP":1009.23,"RH":96.62,"PE":473.9}"""))
    //    spark.sql("select * from power_plant_predictions").show(false)
    //
    //    queue += sc.makeRDD(Seq(s"""{"AT":10.0,"V":40,"AP":1000,"RH":90.0,"PE":0.0}"""))
    //    spark.sql("select * from power_plant_predictions").show(false)

//    val sqlString = "select * from power_plant where at between 10 and 11 and AP between 1000 and 1010 and RH between 90 and 97 and v between 37 and 40 order by PE"
//    spark.sql(sqlString).show(false)

    println("hello, end1")
    val abc = 1

    ssc.start()
    println("hello, end2")
  }
}

case class PowerPlantRow(AT: Double, V : Double, AP : Double, RH : Double, PE : Double)

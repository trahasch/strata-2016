package com.cloudera.spark.churn

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext

import com.cloudera.spark.mllib.SparkConfUtil
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator


object Spark {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spam")
    SparkConfUtil.setConf(conf)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // create dataframe
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter",",")
      .load("data/churn")
    df.show(5)
    df.printSchema()
    df.count()

    val newdf = df.toDF("state", "account_length", "area_code", "phone_number", "international_plan", 
        "voice_mail_plan", 
        "number_vmail_messages", "total_day_minutes", "total_day_calls", "total_day_charge", "total_eve_minutes", 
        "total_eve_calls", "total_eve_charge", "total_night_mins", "total_night_calls", "total_night_charge", 
        "total_intl_minutes", "total_intl_calls", "total_intl_chargs",
                        "num_customer_service_calls", "churned");
    
    newdf.printSchema()
    
    // index churned column
    val churnedIndexer = new StringIndexer().setInputCol("churned").setOutputCol("label")
    val dff_churn = churnedIndexer.fit(newdf).transform(newdf)
    dff_churn.printSchema()
    
    // index international plan column
    val intlPlanIndexer = new StringIndexer().setInputCol("international_plan").setOutputCol("international_plan_indx")
    val dff_intl = intlPlanIndexer.fit(dff_churn).transform(dff_churn)
    dff_intl.printSchema()
    
    // assembler
    val assembler = new VectorAssembler()
      .setInputCols(Array("account_length", "number_vmail_messages", "total_day_minutes", "international_plan_indx"))
      .setOutputCol("features")
    val dff_assembler = assembler.transform(dff_intl)
    dff_assembler.printSchema()
    
    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = dff_assembler.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)
     val rfmodel = rf.fit(trainingData)
    
     // Make predictions.
     val predictions = rfmodel.transform(testData)
    
     // Select example rows to display.
     predictions.select("prediction", "label", "features").show(50)
     
     // Select (prediction, true label) and compute test error
    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderROC")
    val areaUnderROC = evaluator.evaluate(predictions)
    println("areaUnderROC = " + areaUnderROC)

    
     println("Learned classification forest model:\n" + rfmodel.toDebugString)
  }
    
}
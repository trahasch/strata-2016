package com.cloudera.spark.churn

import scala.beans.BeanInfo
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SQLContext
import com.cloudera.spark.mllib.SparkConfUtil
import scala.reflect.runtime.universe


object Spark {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spam")
    SparkConfUtil.setConf(conf)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

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
    
    
  }
    
}
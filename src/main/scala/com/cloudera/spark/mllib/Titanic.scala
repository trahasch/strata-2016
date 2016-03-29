/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.spark.mllib

import com.cloudera.spark.dataset.DatasetTitanic
import com.cloudera.spark.randomforest.JavaRandomForest
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by jayantshekhar
 */
object Titanic {

  // Usage: Titanic <input_file>
  def main(args: Array[String]) {

    // parameters
    var inputFile: String = "data/titanic/train.csv"
    if (args.length > 0) {
      inputFile = args(0)
    }

    // create SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("JavaTitanic")
    SparkConfUtil.setConf(sparkConf)

    // create SparkContent
    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext: SQLContext = new SQLContext(sc)

    // read in the data
    val results: DataFrame = DatasetTitanic.createDF(sqlContext, inputFile)
    // print the DataFrame schema
    results.printSchema

    // read in the data
    val data: JavaRDD[LabeledPoint] = DatasetTitanic.createLabeledPointsRDD(sc, sqlContext, inputFile)

    // split the data for train/test
    val splits: Array[JavaRDD[LabeledPoint]] = data.randomSplit(Array[Double](0.7, 0.3))
    val trainingData: JavaRDD[LabeledPoint] = splits(0)
    val testData: JavaRDD[LabeledPoint] = splits(1)

    // provide details of categorical features
    val categoricalFeaturesInfo: java.util.HashMap[Integer, Integer] = new java.util.HashMap[Integer, Integer]
    categoricalFeaturesInfo.put(0, 2) // feature 0 (male/female) is binary (taking values 0 or 1)

    // classify with random forest
    System.out.println("\nRunning classification using RandomForest\n")
    JavaRandomForest.classifyAndTest(trainingData, testData, categoricalFeaturesInfo)

    // regression with random forest
    System.out.println("\nRunning regression using RandomForest\n")
    JavaRandomForest.testRegression(trainingData, testData, categoricalFeaturesInfo)

    // stop the spark context
    sc.stop
  }
}


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

package com.cloudera.spark.movie;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.cloudera.spark.dataset.DatasetMovieLens;
import com.cloudera.spark.mllib.SparkConfUtil;

/**
 * Created by jayant
 */
public final class JavaDFMovieLensALS {

    public static void main(String[] args) {

        // input parameters
        String inputFile = "data/movielens/ratings";
        int maxIter = 5;
        double regParam = 0.01;

        // spark context
        SparkConf sparkConf = new SparkConf().setAppName("JavaMovieLensALS");
        SparkConfUtil.setConf(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // create data frame
        DataFrame results = DatasetMovieLens.createDF(sqlContext, inputFile);

        // split the dataset
        DataFrame training = results.sample(true, .8);
        DataFrame test = results.sample(true, .2);

        // parametrizing and running the model
        org.apache.spark.ml.recommendation.ALS als = new org.apache.spark.ml.recommendation.ALS();
        als.setUserCol("user").setItemCol("movie").setRatingCol("rating").setMaxIter(maxIter);
        als.setRegParam(regParam);
        ALSModel model =  als.fit(training);

        // show prediction on the test dataset
        DataFrame pred = model.transform(test);
        pred.show();
      
        RegressionEvaluator evaluator = new RegressionEvaluator()
        .setMetricName("rmse")  //Root mean squared error
        .setLabelCol("rating")
        .setPredictionCol("prediction");
        Double rmse = evaluator.evaluate(pred);
        System.out.println("Root-mean-square error = " + rmse);
      
        sc.stop();

    }

}

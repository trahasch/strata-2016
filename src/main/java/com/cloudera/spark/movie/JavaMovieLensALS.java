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

import com.cloudera.spark.dataset.DatasetMovieLens;
import com.cloudera.spark.mllib.SparkConfUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.util.List;

// Usage: JavaMovieLensALS <input_file> <rank> <num_iterations> [<lambda>]
public final class JavaMovieLensALS {

    public static void main(String[] args) {

        // input parameters
        String inputFile = "data/movielens_small";
        int rank = 10;
        int iterations = 10;
        double lambda = 0.01;

        if (args.length > 0) {
            inputFile = args[0];
        }
        if (args.length > 1) {
            rank = Integer.parseInt(args[1]);
        }
        if (args.length > 2) {
            iterations = Integer.parseInt(args[2]);
        }
        if (args.length > 3) {
            lambda = Double.parseDouble(args[3]);
        }

        // spark context
        SparkConf sparkConf = new SparkConf().setAppName("JavaMovieLensALS");
        SparkConfUtil.setConf(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // create RDD
        JavaRDD<Rating> ratings = DatasetMovieLens.createRDD(sc, inputFile);
        ratings.cache();

        // count
        long numUsers = ratings.map(new Function<Rating, Integer>() {
            @Override
            public Integer call(Rating r) {
                return r.user();
            }
        }).distinct().count();
        long numMovies = ratings.map(new Function<Rating, Integer>() {
            @Override
            public Integer call(Rating r) {
                return r.product();
            }
        }).distinct().count();
        long numRatings = ratings.count();

        System.out.println("Num Distinct Users : "+numUsers+" Num Distinct Movies : "+numMovies+" Total Num Ratings : "+numRatings);

        // split into training and test
        double[] weights = {.8, .2};
        JavaRDD<Rating>[] splits = ratings.randomSplit(weights);

        JavaRDD<Rating> training = splits[0];
        JavaRDD<Rating> test = splits[1];

        // count
        long numTrain = training.count();
        long numTest = test.count();
        System.out.println("Num Train : " + numTrain + " Num Test : " + numTest);

        ratings.unpersist();

        // train
        ALS als = new ALS().setRank(rank).setIterations(iterations).setLambda(lambda);
        MatrixFactorizationModel model = als.run(training);

        // print the user features
        JavaRDD userFeaturesRDD = model.userFeatures().toJavaRDD();
        List<Tuple2<Integer, double[]>> ll = userFeaturesRDD.collect();

        System.out.println("Printing User Features");
        for (Tuple2<Integer, double[]> t : ll) {
            String str = "";
            for (double d : t._2())
                str += d + " ";
            System.out.println(t._1() + " : " + str);
        }

        // print the product features
        JavaRDD productFeaturesRDD = model.productFeatures().toJavaRDD();
        ll = productFeaturesRDD.collect();

        System.out.println("\n\nPrinting Product Features");
        for (Tuple2<Integer, double[]> t : ll) {
            String str = "";
            for (double d : t._2())
                str += d + " ";
            System.out.println(t._1() + " : " + str);
        }

        // compute RMSE
        double rmse = computeRmse(model, test);

        System.out.println("Test RMSE = "+rmse);

        sc.stop();

    }

    /** Compute RMSE (Root Mean Squared Error). */
    public static double computeRmse(MatrixFactorizationModel model, JavaRDD<Rating> data) {

        // user product RDD
        JavaPairRDD<Integer, Integer> userProductRDD = data.mapToPair(new PairFunction<Rating, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Rating rating) throws Exception {
                return new Tuple2<Integer, Integer>(rating.user(), rating.product());
            }
        });

        // predict test data
        JavaRDD<Rating> predictions = model.predict(userProductRDD);

        // map test data to pair (user/product) & rating
        JavaPairRDD<Tuple2<Integer, Integer>, Double> dataPair = data.mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>, Double>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {
                return new Tuple2<Tuple2<Integer, Integer>, Double>(
                        new Tuple2<Integer, Integer>(rating.user(), rating.product()), rating.rating());
            }
        });

        // map predictions to pair (user/product) & rating
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictionsPair = predictions.mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>, Double>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {
                return new Tuple2<Tuple2<Integer, Integer>, Double>(
                        new Tuple2<Integer, Integer>(rating.user(), rating.product()), rating.rating());
            }
        });

        // join predictions pair to test data pair
        JavaRDD<Tuple2<Double, Double>> origPredRatingRDD =  predictionsPair.join(dataPair).values();

        // compute rmse
        JavaDoubleRDD errorRDD = origPredRatingRDD.mapToDouble(new DoubleFunction<Tuple2<Double, Double>>() {
            @Override
            public double call(Tuple2<Double, Double> doubleDoubleTuple2) throws Exception {
                return (doubleDoubleTuple2._1() - doubleDoubleTuple2._2()) * (doubleDoubleTuple2._1() - doubleDoubleTuple2._2());
            }
        });

        double rmse = Math.sqrt(errorRDD.mean());

        return rmse;
    }


}
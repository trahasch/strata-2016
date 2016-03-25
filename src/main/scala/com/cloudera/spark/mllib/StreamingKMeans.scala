package com.cloudera.spark.mllib

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


import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Estimate clusters on one stream of data and make predictions
  * on another stream, where the data streams arrive as text files
  * into two different directories.
  *
  * The rows of the training text files must be vector data in the form
  * `[x1,x2,x3,...,xn]`
  * Where n is the number of dimensions.
  *
  * The rows of the test text files must be labeled data in the form
  * `(y,[x1,x2,x3,...,xn])`
  * Where y is some identifier. n must be the same for train and test.
  *
  * Usage:
  *   StreamingKMeans <trainingDir> <testDir> <batchDuration> <numClusters> <numDimensions>
  *
  * To run on your local machine using the two directories `trainingDir` and `testDir`,
  * with updates every 5 seconds, 2 dimensions per data point, and 3 clusters, call:
  *    $ bin/run-example mllib.StreamingKMeansExample trainingDir testDir 5 3 2
  *
  * As you add text files to `trainingDir` the clusters will continuously update.
  * Anytime you add text files to `testDir`, you'll see predicted labels using the current model.
  *
  */
object StreamingKMeans {

  // Usage: StreamingKMeansExample <trainingDir> <testDir> <batchDuration> <numClusters> <numDimensions>
  def main(args: Array[String]) {

    var trainingDir = "trainingDir"
    var testDir = "testDir"
    var batchDuration : Long = 5
    var numClusters = 3
    var numDimensions = 3

    if (args.length > 0) {
      trainingDir = args(0)
    }
    if (args.length > 1) {
      testDir = args(1)
    }
    if (args.length > 2) {
      batchDuration = args(2).toLong
    }
    if (args.length > 3) {
      numClusters = args(3).toInt
    }
    if (args.length > 4) {
      numDimensions = args(4).toInt
    }

    val conf = new SparkConf().setMaster("local").setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(batchDuration))

    // train the model on this data
    val trainingData = ssc.textFileStream(trainingDir).map(Vectors.parse)

    // test the model on this data
    val testData = ssc.textFileStream(testDir).map(LabeledPoint.parse)

    val model = new StreamingKMeans()
      .setK(numClusters)
      .setDecayFactor(1.0)
      .setRandomCenters(numDimensions, 0.0)

    model.trainOn(trainingData) // train the model

    // parameter : scala.Tuple2[K, org.apache.spark.mllib.linalg.Vector]
    // returns   : org.apache.spark.streaming.dstream.DStream[scala.Tuple2[K, scala.Int]]
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()
  }
}


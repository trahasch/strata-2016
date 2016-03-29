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

object StreamingKMeansTrainTest {

  // Usage: StreamingKMeansTrainTest <trainingDir> <testDir> <batchDuration> <numClusters> <numDimensions>
  def main(args: Array[String]) {

    var trainingDir = "streamingTrainDir"
    var testDir = "streamingTestDir"
    var batchDuration : Long = 15 // in seconds
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

    // create StreamingContext
    val conf = new SparkConf().setMaster("local").setAppName("StreamingKMeansExample")
    SparkConfUtil.setConf(conf)
    val ssc = new StreamingContext(conf, Seconds(batchDuration))

    // train the model on this data
    val trainingData = ssc.textFileStream(trainingDir).map(Vectors.parse)

    // print training data
    trainingData.print(5)

    // test the model on this data
    val testData = ssc.textFileStream(testDir).map(Vectors.parse)
    testData.print(5)

    val model = new StreamingKMeans()
      .setK(numClusters)
      .setDecayFactor(1.0)
      .setRandomCenters(numDimensions, 0.0)

    // Update the clustering model by training on batches of data from a DStream
    model.trainOn(trainingData) // train the model

    // predict
    model.predictOn(testData).print()

    ssc.start()
    ssc.awaitTermination()
  }
}


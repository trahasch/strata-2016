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

package com.cloudera.spark

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

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.feature.StandardScaler

object LinearRegressionMLLib {

  case class X(
                id: String ,price: Double, lotsize: Double, bedrooms: Double,
                bathrms: Double,stories: Double, driveway: String,recroom: String,
                fullbase: String, gashw: String, airco: String, garagepl: Double, prefarea: String)

  def main (args: Array[String]) {

    var input = data/housing/housing.regression"
    if (args.length > 0) {
	input = args(0)
    }

    val sparkConf = new SparkConf().setAppName("LinearRegressionMLLib")

    val sc = new SparkContext(sparkConf)

    val data = sc.textFile(args(0))

    val parsedData = data.map { line =>
       val parts = line.split(',')
       val label = parts(0).toDouble
       val features = parts(1).split(' ').map(_.toDouble)
       LabeledPoint(label, Vectors.dense(features))
    }.cache()

    val scaler = new StandardScaler(withMean = true, withStd = true).fit(parsedData.map(x => x.features))

    val scaledData = parsedData.map(x =>
        LabeledPoint(x.label,
        scaler.transform(Vectors.dense(x.features.toArray))))

    val numIterations = 100
    val stepSize = 0.1

    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer.setNumIterations(numIterations).setStepSize(stepSize)
    val model = algorithm.run(scaledData)

    scaledData.take(5).foreach{ x=> println(s"Predicted: ${model.predict(x.features)}, Label: ${x.label}")}

    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val MSE = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2) }.mean()

  }
}

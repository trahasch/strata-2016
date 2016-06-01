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

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Created by jayant
 */
object Random {

  def main(args: Array[String]) {

    random()

  }

  def random(): Unit = {
    println("======================================")
    println("|             Random                 |")
    println("======================================")

    val sparkConf: SparkConf = new SparkConf().setAppName("Random")
    SparkConfUtil.setConf(sparkConf)
    val sc: SparkContext = new SparkContext(sparkConf)

    val normalRDD = RandomRDDs.normalRDD(sc, 1000, 10)

    val rdd = RandomRDDs.normalVectorRDD(sc, 100, 3, 1)

    // clean the directories
    FileUtils.deleteDirectory(new File("streamingTrainDir"))
    mkdir("streamingTrainDir")
    FileUtils.deleteDirectory(new File("streamingTestDir"))
    mkdir("streamingTestDir")
    FileUtils.deleteDirectory(new File("streamingDataDir"))
    mkdir("streamingDataDir")

    var idx = 1;
    val datadir = "streamingDataDir/";
    while (true) {

      // generate training data
      val trainrdd = RandomRDDs.normalVectorRDD(sc, 100, 3, 1)
      trainrdd.saveAsTextFile(datadir+idx)
      mv(datadir+idx+"/part-00000", "streamingTrainDir/"+idx)

      idx += 1
      Thread.sleep(15000)

      // generate test data
      val rdd = RandomRDDs.normalVectorRDD(sc, 100, 3, 1)
      val lprdd = rdd.map { x => new LabeledPoint((Math.random()*100).toInt % 3, x) }
      
      lprdd.saveAsTextFile(datadir+idx)
      mv(datadir+idx+"/part-00000", "streamingTestDir/"+idx)

      idx += 1
      Thread.sleep(15000)
    }

    sc.stop()

  }

  def mkdir(name : String): Unit = {
    val dir = new File(name);
    dir.mkdir()
  }

  def mv(oldName: String, newName: String) =
    Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)
}
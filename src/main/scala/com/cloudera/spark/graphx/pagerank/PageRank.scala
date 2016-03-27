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

package com.cloudera.spark.graphx.pagerank

import com.cloudera.spark.graphx.dataset.{DatasetUsers, DatasetFollowers, DatasetSimpleGraph}
import com.cloudera.spark.mllib.SparkConfUtil
import org.apache.spark.{SparkContext, SparkConf}

object PageRank {

  def main(args: Array[String]) {

    pageRank()

  }

  def pageRank(): Unit = {
    println("======================================")
    println("|             Page Rank              |")
    println("======================================")

    val sparkConf: SparkConf = new SparkConf().setAppName("PageRank")
    SparkConfUtil.setConf(sparkConf)
    val sc: SparkContext = new SparkContext(sparkConf)

    // Load the edges as a graph
    val graph = DatasetFollowers.graph(sc)

    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices

    // Join the ranks with the usernames
    val users = DatasetUsers.users(sc)

    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    // Print the result
    println(ranksByUsername.collect().mkString("\n"))

    sc.stop()
  }
}

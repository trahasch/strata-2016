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

package com.cloudera.spark.graphx.wikipedia;

import com.cloudera.spark.mllib.SparkConfUtil
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WikipediaPageRankFormat2 {

  def main(args: Array[String]): Unit = {

    // spark context
    val sparkConf: SparkConf = new SparkConf().setAppName("Wikipedia Page Rank")
    SparkConfUtil.setConf(sparkConf)
    val sc: SparkContext = new SparkContext(sparkConf)

    //Raw files being read : http://haselgrove.id.au/wikipedia.htm
    val rawLinks:RDD[String] = sc.textFile("datagraphx/wikipediaV2/links-simple-sorted-top100.txt");
    val rawVertex:RDD[String] = sc.textFile("datagraphx/wikipediaV2/titles-sorted-top500.txt");

    //Create the edge RDD
    val edges = rawLinks.flatMap(line => {
      val Array(key, values) = line.split(":",2)
      for(value <- values.trim.split("""\s+"""))
        yield (Edge(key.toLong, value.trim.toLong, 0))
    })

    val numberOfEdges = edges.count()
    println("Number of edges:"+ numberOfEdges)

    //Create the vertex RDD
    val vertices = rawVertex.zipWithIndex().map({ row =>  // ((a,0),(b,1),(c,2))
      val nodeName = row._1
      val nodeIndex = row._2
      (nodeIndex.toLong,nodeName)                         // ((0,a),(1,b),(2,c))
    });

    val numberOfVertices = vertices.count()
    println("Number of vertices:"+ numberOfVertices)

    //Create the graph object
    val graph = Graph(vertices, edges, "").cache()

    //Run page rank
    val wikiGraph = graph.pageRank(0.01).cache() // tolerance required for convergence

    // print the vertex ids and their page ranks of the first 20 vertices
    wikiGraph.vertices.takeOrdered(20).foreach(println(_))

  }


}
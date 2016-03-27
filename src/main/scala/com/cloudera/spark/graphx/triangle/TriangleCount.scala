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

package com.cloudera.spark.graphx.triangle

import com.cloudera.spark.graphx.dataset.DatasetSimpleGraph
import com.cloudera.spark.mllib.SparkConfUtil
import org.apache.spark.{SparkConf, SparkContext}

object TriangleCount {

  def main(args: Array[String]) {

    triangleCount()

  }

  def triangleCount(): Unit = {
    println("======================================")
    println("|             Triangle Count         |")
    println("======================================")

    val sparkConf: SparkConf = new SparkConf().setAppName("Test")
    SparkConfUtil.setConf(sparkConf)
    val sc: SparkContext = new SparkContext(sparkConf)

    val graph = DatasetSimpleGraph.graph(sc)

    val outdegrees = graph.outDegrees;
    val out = outdegrees.collect();
    println(out.mkString(" "))

    val indegrees = graph.inDegrees;
    val in = indegrees.collect();
    println(in.mkString(" "))

    // number of triangles passing through each vertex
    val triangles = graph.triangleCount()

    val numv = triangles.numVertices
    println("Number of Vertices " + numv)

    println("Triangles: " + triangles.vertices.map {
      case (vid, data) => data.toLong
    }.reduce(_ + _) / 3)

    sc.stop()
  }
}
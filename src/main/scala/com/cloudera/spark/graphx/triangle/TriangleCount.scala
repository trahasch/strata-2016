package com.cloudera.spark.graphx.triangle

import com.cloudera.spark.graphx.dataset.DatasetSimpleGraph
import com.cloudera.spark.mllib.SparkConfUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by jayant
 */

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
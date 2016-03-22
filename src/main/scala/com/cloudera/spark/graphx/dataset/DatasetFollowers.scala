package com.cloudera.spark.graphx.dataset

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{GraphLoader, Edge, Graph}

object DatasetFollowers {

  def graph(sc : SparkContext): Graph[Int, Int] = {

    val graph = GraphLoader.edgeListFile(sc, "datagraphx/followers")

    graph

  }
}
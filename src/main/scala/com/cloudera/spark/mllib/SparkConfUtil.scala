package com.cloudera.spark.mllib

import org.apache.spark.SparkConf

/**
 * Created by jayant
 */
object SparkConfUtil {

  val isLocal = true;

  def setConf(conf: SparkConf): Unit = {

    if (isLocal) {
     conf.setMaster("local")
     conf.set("spark.broadcast.compress", "false")
     conf.set("spark.shuffle.compress", "false")
    }
  }
}

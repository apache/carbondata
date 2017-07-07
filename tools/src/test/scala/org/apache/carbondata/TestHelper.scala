package org.apache.carbondata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestHelper {

  val conf: SparkConf = new SparkConf().setAppName("cardinality_demo").setMaster("local")
  val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

}

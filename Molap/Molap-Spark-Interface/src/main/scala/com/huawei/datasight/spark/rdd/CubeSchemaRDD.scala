package com.huawei.datasight.spark.rdd

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


trait CubeSchemaRDDLike {
  def getMeasures: Array[String]

  def getDimensions: Array[String]
}

/*
 * Check the possibility of using implicit to convert any SchemaRDD into Cube RDD
 * */
class CubeSchemaRDD(sqlContext: SQLContext,
                    logicalPlan: LogicalPlan) extends SchemaRDD(sqlContext, logicalPlan) with CubeSchemaRDDLike {
  override def getMeasures: Array[String] = {
    Array("HI")
  }

  override def getDimensions: Array[String] = {
    Array("HI")
  }


}
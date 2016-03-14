/**
  *
  */
package com.huawei.datasight.spark.dsl.interpreter

import org.apache.spark.SparkContext
import org.apache.spark.sql.OlapContext
import org.apache.spark.sql.SchemaRDD
import com.huawei.datasight.spark.processors.TransformHolder

/**
  * @author R00900208
  *
  */

case class EvaluatorAttributeModel(sc: SparkContext, olapContext: OlapContext, trans: TransformHolder) {

}
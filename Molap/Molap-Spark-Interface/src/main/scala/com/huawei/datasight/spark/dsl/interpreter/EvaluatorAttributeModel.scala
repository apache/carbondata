/**
  *
  */
package com.huawei.datasight.spark.dsl.interpreter

import com.huawei.datasight.spark.processors.TransformHolder
import org.apache.spark.SparkContext
import org.apache.spark.sql.OlapContext

case class EvaluatorAttributeModel(sc: SparkContext, olapContext: OlapContext, trans: TransformHolder)

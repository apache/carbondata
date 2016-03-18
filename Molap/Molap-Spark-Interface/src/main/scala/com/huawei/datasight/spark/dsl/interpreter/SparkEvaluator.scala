/**
  *
  */
package com.huawei.datasight.spark.dsl.interpreter

import com.huawei.datasight.spark.processors.TransformHolder

trait SparkEvaluator extends Serializable {
  def getRDD(regAttrModel: EvaluatorAttributeModel): TransformHolder
}
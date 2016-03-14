/**
  *
  */
package com.huawei.datasight.spark.dsl.interpreter

import org.apache.spark.rdd.RDD
import com.huawei.datasight.spark.processors.TransformHolder


/**
  * @author R00900208
  *
  */
trait SparkEvaluator extends Serializable {
  def getRDD(regAttrModel: EvaluatorAttributeModel): TransformHolder
}
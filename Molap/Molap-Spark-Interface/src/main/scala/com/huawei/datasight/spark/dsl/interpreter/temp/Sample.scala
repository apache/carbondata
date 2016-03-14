package com.huawei.datasight.spark.dsl.interpreter.temp

import org.apache.spark.SparkContext
import org.apache.spark.sql.OlapContext
import org.apache.spark.rdd.RDD
import com.huawei.datasight.spark.dsl.interpreter.SparkEvaluator
import com.huawei.datasight.spark.dsl.interpreter.EvaluatorAttributeModel
import org.apache.spark.sql.SchemaRDD
import com.huawei.datasight.spark.processors.TransformHolder

class Sample extends SparkEvaluator {
  def getRDD(regAttrModel: EvaluatorAttributeModel): TransformHolder = {
    val sc: SparkContext = regAttrModel.sc
    val olapContext: OlapContext = regAttrModel.olapContext
    val currRdd = regAttrModel.trans.rdd.asInstanceOf[SchemaRDD]
    val rdd = {
      currRdd.map(x => x)
    }
    TransformHolder(rdd, regAttrModel.trans.mataData)
  }
}
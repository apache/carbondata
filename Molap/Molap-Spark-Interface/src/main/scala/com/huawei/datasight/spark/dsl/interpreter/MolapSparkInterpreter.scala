/**
  *
  */
package com.huawei.datasight.spark.dsl.interpreter


import java.io.File

import com.huawei.datasight.spark.processors.TransformHolder

class MolapSparkInterpreter {

  def evaluate(code: String, regAttrModel: EvaluatorAttributeModel): TransformHolder = {
    val exec = new Eval(Some(new File("temp"))).getInstance[SparkEvaluator](code)
    val trans = exec.getRDD(regAttrModel)
    trans
  }

}
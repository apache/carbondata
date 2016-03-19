/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
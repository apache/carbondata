/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.carbondata.spark.agg

import org.apache.spark.sql.types._

import org.carbondata.query.aggregator.MeasureAggregator

/**
 * class to support user defined type for carbon measure aggregators
 * from spark 1.5, spark has made the data type strict and ANY is no more supported
 * for every data, we need to give the data type
 */
class MeasureAggregatorUDT extends UserDefinedType[MeasureAggregator] {
  // the default DoubleType is Ok as we are not going to pass to spark sql to
  // evaluate,need to add this for compilation errors
  override def sqlType: DataType = ArrayType(DoubleType, false)

  override def serialize(obj: Any): Any = {
    obj match {
      case p: MeasureAggregator => p
    }
  }

  override def deserialize(datum: Any): MeasureAggregator = {
    datum match {
      case values =>
        val xy = values.asInstanceOf[MeasureAggregator]
        xy
    }
  }

  override def userClass: Class[MeasureAggregator] = classOf[MeasureAggregator]

  override def asNullable: MeasureAggregatorUDT = this
}

case object MeasureAggregatorUDT extends MeasureAggregatorUDT

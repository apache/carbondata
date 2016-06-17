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

package org.apache.spark.sql.agg

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

case class CarbonAvgHelperExpression(
    left: Expression,
    right: Expression,
    index: Int,
    dataType: DataType)
  extends BinaryExpression with CodegenFallback {

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val left = input1 match {
      case g: ArrayData => g.getDouble(index)
      case others => others.toString.toDouble
    }
    val right = input2 match {
      case g: ArrayData => g.getDouble(index)
      case others => others.toString.toDouble
    }
    if (index == 1) {
      (left + right).toLong
    } else {
      dataType match {
        case d: DecimalType =>
        case _ => left + right
      }
    }
  }
}

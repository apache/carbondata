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

package com.huawei.datasight.spark.rdd

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.BinaryArithmetic
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.UnaryExpression

object SqlUdfLib {

  def sample(row: Row): Any = {
    row.getDouble(0) * 100
  }

  def range(row: Row): Any = {
    var value = row.getDouble(0);
    if (value >= 0 && value < 10) "0-10"
    else if (value >= 10 && value < 20) "10-20"
    else if (value >= 20 && value < 30) "20-30"
    else if (value >= 30 && value < 40) "30-40"
    else if (value >= 40 && value < 50) "40-50"
    else if (value >= 50 && value < 60) "50-60"
    else if (value >= 60 && value < 70) "60-70"
    else if (value >= 70 && value < 80) "70-80"
    else if (value >= 80 && value < 90) "80-90"
    else if (value >= 90 && value < 100) "90-100"
    else ">=100"
  }


}

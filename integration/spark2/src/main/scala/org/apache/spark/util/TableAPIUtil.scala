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

package org.apache.spark.util

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
 * table api util
 */
object TableAPIUtil {
  def parseSchemaName(tableName: String): (String, String) = {
    if (tableName.contains(".")) {
      val parts = tableName.split(".")
      (parts(0), parts(1))
    } else {
      ("default", tableName)
    }
  }

  def escape(str: String): String = {
    val newStr = str.trim
    if (newStr.startsWith("\"") && newStr.endsWith("\"")) {
      newStr.substring(1, newStr.length - 1)
    } else {
      str
    }
  }

  def spark(storePath: String, appName: String): SparkSession = {
    SparkSession
        .builder
        .appName(appName)
        .master("local")
        .config(CarbonCommonConstants.STORE_LOCATION, storePath)
        .getOrCreate()
  }
}

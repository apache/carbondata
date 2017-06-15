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

package org.apache.carbondata.examples

import scala.collection.mutable.LinkedHashMap

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object CarbonShowPartitionInfo {
  def main(args: Array[String]) {

    CarbonShowPartitionInfo.extracted("t3", args)
  }
  def extracted(tableName: String, args: Array[String]): Unit = {
    val cc = ExampleUtils.createCarbonContext("CarbonShowPartitionInfo")
    val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"
    cc.sql(s"DROP TABLE IF EXISTS $tableName")
    cc.sql(s"""
                | CREATE TABLE IF NOT EXISTS $tableName
                | (
                | logdate Timestamp,
                | phonenumber Int,
                | country String,
                | area String
                | )
                | PARTITIONED BY (vin String)
                | STORED BY 'carbondata'
                | TBLPROPERTIES('PARTITION_TYPE'='HASH','NUM_PARTITIONS'='5')
                """.stripMargin)

    cc.sql(s"DROP TABLE IF EXISTS partitionDB.$tableName")
    cc.sql(s"DROP DATABASE IF EXISTS partitionDB")
    cc.sql(s"CREATE DATABASE partitionDB")
    cc.sql(s"""
                | CREATE TABLE IF NOT EXISTS partitionDB.$tableName
                | (
                | logdate Timestamp,
                | phonenumber Int,
                | country String,
                | area String
                | )
                | PARTITIONED BY (vin String)
                | STORED BY 'carbondata'
                | TBLPROPERTIES('PARTITION_TYPE'='HASH','NUM_PARTITIONS'='5')
                """.stripMargin)
    cc.sql(s"""
      SHOW PARTITIONS partitionDB.$tableName
             """).show(10000)

    cc.sql(s"""
      SHOW PARTITIONS $tableName
             """).show(10000)

  }
}

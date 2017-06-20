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

object ShowPartitionInfoExample {
  def main(args: Array[String]) {
    ShowPartitionInfoExample.extracted("t3", args)
  }
  def extracted(tableName: String, args: Array[String]): Unit = {
    val cc = ExampleUtils.createCarbonContext("CarbonShowPartitionInfo")
    val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    // range partition
    cc.sql("DROP TABLE IF EXISTS t1")

    cc.sql("""
                | CREATE TABLE IF NOT EXISTS t1(
                | vin STRING,
                | phonenumber INT,
                | country STRING,
                | area STRING
                | )
                | PARTITIONED BY (logdate TIMESTAMP)
                | STORED BY 'carbondata'
                | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
                | 'RANGE_INFO'='2014/01/01,2015/01/01,2016/01/01')
              """.stripMargin)
    cc.sql("""SHOW PARTITIONS t1""").show()

    cc.sql("""
                | CREATE TABLE IF NOT EXISTS t3(
                | logdate Timestamp,
                | phonenumber Int,
                | country String,
                | area String
                | )
                | PARTITIONED BY (vin String)
                | STORED BY 'carbondata'
                | TBLPROPERTIES('PARTITION_TYPE'='HASH','NUM_PARTITIONS'='5')
                """.stripMargin)
    cc.sql("""SHOW PARTITIONS t3""").show()
    // list partition
    cc.sql("DROP TABLE IF EXISTS t5")

    cc.sql("""
               | CREATE TABLE IF NOT EXISTS t5(
               | vin String,
               | logdate Timestamp,
               | phonenumber Int,
               | area String
               | )
               | PARTITIONED BY (country string)
               | STORED BY 'carbondata'
               | TBLPROPERTIES('PARTITION_TYPE'='LIST',
               | 'LIST_INFO'='(China,United States),UK ,japan,(Canada,Russia), South Korea ')
       """.stripMargin)
    cc.sql("""SHOW PARTITIONS t5""").show()

    cc.sql(s"DROP TABLE IF EXISTS partitionDB.$tableName")
    cc.sql(s"DROP DATABASE IF EXISTS partitionDB")
    cc.sql(s"CREATE DATABASE partitionDB")
    cc.sql(s"""
                | CREATE TABLE IF NOT EXISTS partitionDB.$tableName(
                | logdate Timestamp,
                | phonenumber Int,
                | country String,
                | area String
                | )
                | PARTITIONED BY (vin String)
                | STORED BY 'carbondata'
                | TBLPROPERTIES('PARTITION_TYPE'='HASH','NUM_PARTITIONS'='5')
                """.stripMargin)
    cc.sql(s"""SHOW PARTITIONS partitionDB.$tableName""").show()
    cc.sql(s"""SHOW PARTITIONS $tableName""").show()

    cc.sql("DROP TABLE IF EXISTS t1")
    cc.sql("DROP TABLE IF EXISTS t3")
    cc.sql("DROP TABLE IF EXISTS t5")
    cc.sql(s"DROP TABLE IF EXISTS partitionDB.$tableName")
    cc.sql(s"DROP DATABASE IF EXISTS partitionDB")

  }
}

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

import org.apache.spark.sql.AnalysisException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object CarbonPartitionExample {

  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("CarbonPartitionExample")
    val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    // none partition table
    cc.sql("DROP TABLE IF EXISTS t0")
    cc.sql("""
                | CREATE TABLE IF NOT EXISTS t0
                | (
                | vin String,
                | logdate Timestamp,
                | phonenumber Int,
                | country String,
                | area String
                | )
                | STORED BY 'carbondata'
              """.stripMargin)

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

    // hash partition
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

    cc.sql(s"DROP TABLE IF EXISTS partitionDB.t9")
    cc.sql(s"DROP DATABASE IF EXISTS partitionDB")
    cc.sql(s"CREATE DATABASE partitionDB")
    cc.sql(s"""
                | CREATE TABLE IF NOT EXISTS partitionDB.t9(
                | logdate Timestamp,
                | phonenumber Int,
                | country String,
                | area String
                | )
                | PARTITIONED BY (vin String)
                | STORED BY 'carbondata'
                | TBLPROPERTIES('PARTITION_TYPE'='HASH','NUM_PARTITIONS'='5')
                """.stripMargin)
    // hive partition table
    cc.sql("DROP TABLE IF EXISTS t7")
    cc.sql("""
       | create table t7(id int, name string) partitioned by (city string)
       | row format delimited fields terminated by ','
       """.stripMargin)
    cc.sql("alter table t7 add partition (city = 'Hangzhou')")
    // hive partition table
    cc.sql(s"DROP TABLE IF EXISTS hiveDB.t7")
    cc.sql(s"CREATE DATABASE IF NOT EXISTS hiveDB")
    cc.sql("""
       | create table hiveDB.t7(id int, name string) partitioned by (city string)
       | row format delimited fields terminated by ','
       """.stripMargin)
    cc.sql("alter table hiveDB.t7 add partition (city = 'Shanghai')")
    //  show partitions
    try {
      cc.sql("SHOW PARTITIONS t0").show(100, false)
    } catch {
      case ex: AnalysisException => LOGGER.error(ex.getMessage())
    }
    cc.sql("SHOW PARTITIONS t1").show(100, false)
    cc.sql("SHOW PARTITIONS t3").show(100, false)
    cc.sql("SHOW PARTITIONS t5").show(100, false)
    cc.sql("SHOW PARTITIONS t7").show(100, false)
    cc.sql("use hiveDB").show()
    cc.sql("SHOW PARTITIONS t7").show(100, false)
    cc.sql("use default").show()
    cc.sql("SHOW PARTITIONS partitionDB.t9").show(100, false)

    cc.sql("DROP TABLE IF EXISTS t0")
    cc.sql("DROP TABLE IF EXISTS t1")
    cc.sql("DROP TABLE IF EXISTS t3")
    cc.sql("DROP TABLE IF EXISTS t5")
    cc.sql("DROP TABLE IF EXISTS t7")
    cc.sql(s"DROP TABLE IF EXISTS hiveDb.t7")
    cc.sql(s"DROP TABLE IF EXISTS partitionDB.t9")
    cc.sql(s"DROP DATABASE IF EXISTS partitionDB")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }
}

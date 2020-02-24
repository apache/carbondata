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

package org.apache.carbondata.spark.testsuite.partition

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.spark.exception.ProcessMetaDataException

class TestShowPartition  extends QueryTest with BeforeAndAfterAll {
  override def beforeAll = {

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")

    sql("drop table if exists notPartitionTable")
    sql("""
                | CREATE TABLE notPartitionTable
                | (
                | vin String,
                | logdate Timestamp,
                | phonenumber Int,
                | country String,
                | area String
                | )
                | STORED AS carbondata
              """.stripMargin)

    sql(s"CREATE DATABASE if not exists partitionDB")

    sql("DROP TABLE IF EXISTS hiveTable")
    sql("""
       | create table hiveTable(id int, name string) partitioned by (city string)
       | row format delimited fields terminated by ','
       """.stripMargin)
    sql("alter table hiveTable add partition (city = 'Hangzhou')")

    sql(s"CREATE DATABASE if not exists hiveDB")
    sql("DROP TABLE IF EXISTS hiveDB.hiveTable")
    sql("""
       | create table hiveDB.hiveTable(id int, name string) partitioned by (city string)
       | row format delimited fields terminated by ','
       """.stripMargin)
    sql("alter table hiveDB.hiveTable add partition (city = 'Shanghai')")
  }

  test("show partition table: exception when show not partition table") {
    val errorMessage = intercept[AnalysisException] {
      sql("show partitions notPartitionTable").show()
    }
    assert(errorMessage.getMessage.contains(
      "SHOW PARTITIONS is not allowed on a table that is not partitioned"))
  }

  test("show partition table: hive partition table") {
    // EqualTo
    checkAnswer(sql("show partitions hiveTable"), Seq(Row("city=Hangzhou")))
    sql("use hiveDB").show()
    checkAnswer(sql("show partitions hiveTable"), Seq(Row("city=Shanghai")))
    sql("use default").show()
  }

  override def afterAll = {
    sql("use default")
    sql("drop table if exists notPartitionTable")
    sql("drop table if exists  hiveTable")
    try {
      sql("drop table if exists  hiveDB.hiveTable")
    } catch {
      case ex: NoSuchDatabaseException => print(ex.getMessage())
    }
    sql("DROP DATABASE if exists partitionDB")
    sql("DROP DATABASE if exists hiveDB")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }
}

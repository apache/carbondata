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

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.spark.sql.AnalysisException
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

class TestLoadDataWithDictionaryExcludeAndInclude extends QueryTest with BeforeAndAfterAll {
  var filePath: String = _
  var pwd: String = _

  def buildTestData() = {
    filePath = s"$resourcesPath/emptyDimensionData.csv"
  }

  def dropTable() = {
    sql("DROP TABLE IF EXISTS exclude_include_t3")
    sql("DROP TABLE IF EXISTS exclude_include_hive_t3")
  }

  def buildTable() = {
    try {
      sql(
        """
           CREATE TABLE exclude_include_hive_t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           row format delimited fields terminated by ','
        """)
      sql(
        """
           CREATE TABLE exclude_include_t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED AS carbondata
        """)
    } catch {
      case ex: Throwable => LOGGER.error(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
  }

  def loadTable() = {
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      sql(
        s"""
           LOAD DATA LOCAL INPATH '$filePath' into table exclude_include_t3
           """)
      sql(
        s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/emptyDimensionDataHive.csv' into table exclude_include_hive_t3
           """)
    } catch {
      case ex: Throwable => LOGGER.error(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
  }

  override def beforeAll {
    dropTable
    buildTestData
    buildTable
    loadTable
  }

  test("test load data with dictionary exclude & include and with empty dimension") {
    checkAnswer(
      sql("select ID from exclude_include_t3"), sql("select ID from exclude_include_hive_t3")
    )
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    dropTable
  }
}


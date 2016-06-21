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

package org.carbondata.spark.testsuite.dataload

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties


class TestLoadDataWithDictionaryExcludeAndInclude extends QueryTest with BeforeAndAfterAll {
  var filePath: String = _
  var pwd: String = _

  def buildTestData() = {
    pwd = new File(this.getClass.getResource("/").getPath + "/../../").getCanonicalPath
    filePath = pwd + "/src/test/resources/emptyDimensionData.csv"
  }

  def buildTable() = {
    try {
      sql(
        """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'org.apache.carbondata.format'
           TBLPROPERTIES('DICTIONARY_EXCLUDE'='country,phonetype,serialname',
           'DICTIONARY_INCLUDE'='ID')
        """)
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
  }

  def loadTable() = {
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      sql(
        s"""
           LOAD DATA LOCAL INPATH '$filePath' into table t3
           """)
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
  }

  override def beforeAll {
    buildTestData
    buildTable
    loadTable
  }

  test("test load data with dictionary exclude & include and with empty dimension") {
    checkAnswer(
      sql("select ID from t3"), Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(6), Row(7),
        Row(8), Row(9), Row(10), Row(11), Row(12), Row(13), Row(14), Row(15), Row(16), Row
        (17), Row(18), Row(19), Row(20))
    )
  }

  override def afterAll {
    sql("drop table t3")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}


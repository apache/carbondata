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

package org.apache.spark.carbondata.query

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test cases for testing columns having \N or \null values for non numeric columns
 */
class TestNotEqualToFilter extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists test_not_equal_to_carbon")
    sql("drop table if exists test_not_equal_to_hive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
      )
    sql(
      """
        CREATE TABLE IF NOT EXISTS test_not_equal_to_carbon
        (ID Int, date Timestamp, country String,
        name String, phonetype String, serialname String, salary Int)
        STORED AS carbondata
      """)
    sql(
      """
        CREATE TABLE IF NOT EXISTS test_not_equal_to_hive
        (ID Int, date Timestamp, country String,
        name String, phonetype String, serialname String, salary Int)
        row format delimited fields terminated by ','
      """)
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/filter/notEqualToFilter.csv' into table
           test_not_equal_to_carbon
           OPTIONS('FILEHEADER'='ID,date,country,name,phonetype,serialname,salary')
           """)
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/filter/notEqualToFilter.csv' into table
           test_not_equal_to_hive
           """)
  }

  test("select Id from test_not_equal_to_carbon where id != '7'") {
    checkAnswer(
      sql("select Id from test_not_equal_to_carbon where id != '7'"),
      sql("select Id from test_not_equal_to_hive where id != '7'")
    )
  }

  test("select Id from test_not_equal_to_carbon where id != 7.0") {
    checkAnswer(
      sql("select Id from test_not_equal_to_carbon where id != 7.0"),
      sql("select Id from test_not_equal_to_hive where id != 7.0")
    )
  }

  test("select Id from test_not_equal_to_carbon where id != 7") {
    checkAnswer(
      sql("select Id from test_not_equal_to_carbon where id != 7"),
      sql("select Id from test_not_equal_to_hive where id != 7")
    )
  }

  override def afterAll {
    sql("drop table if exists test_not_equal_to_carbon")
    sql("drop table if exists test_not_equal_to_hive")
  }
}

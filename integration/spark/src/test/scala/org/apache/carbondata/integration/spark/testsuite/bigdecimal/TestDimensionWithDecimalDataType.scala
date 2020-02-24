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

package org.apache.carbondata.spark.testsuite.bigdecimal

import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
 * Test cases for testing big decimal functionality on dimensions
 */
class TestDimensionWithDecimalDataType extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql("CREATE TABLE IF NOT EXISTS carbonTable (ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary Decimal(19,2))STORED AS carbondata ")
    sql("create table if not exists hiveTable(ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary Decimal(19,2))row format delimited fields terminated by ','")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/decimalDataWithHeader.csv' into table carbonTable")
    sql(s"LOAD DATA local inpath '$resourcesPath/decimalDataWithoutHeader.csv' INTO table hiveTable")
  }

  test("test unsafe with bigdecimal") {
    sql("drop table if exists unsafecarbonTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "true")
    sql("CREATE TABLE IF NOT EXISTS unsafecarbonTable (ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary Decimal(19,2))STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/decimalDataWithHeader.csv' into table unsafecarbonTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "false")
    sql("drop table if exists unsafecarbonTable")
  }

  test("test aggregate query on dimension column with decimal data type") {
    checkAnswer(sql("select sum(salary) from carbonTable"),
      sql("select sum(salary) from hiveTable"))
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
  }
}

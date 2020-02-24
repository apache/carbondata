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

package org.apache.carbondata.spark.testsuite.filterexpr

import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
 * Test cases for testing columns having \N or \null values for non numeric columns
 */
class TestAndEqualFilterEmptyOperandValue extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
      )
    val csvFilePath = s"$resourcesPath/filter/datagrtlrt.csv"
    sql(
      "CREATE TABLE IF NOT EXISTS carbonTable(date Timestamp, country String, salary Int) " +
      "STORED AS carbondata"
    )
    sql(
      "create table if not exists hiveTable(date Timestamp, country String, salary Int)row format" +
      " delimited fields " +
      "terminated by ','"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + csvFilePath + "' into table carbonTable OPTIONS " +
      "('FILEHEADER'='date,country,salary')"
    )
    sql(
      "LOAD DATA local inpath '" + csvFilePath + "' INTO table hiveTable"
    )
  }

  test("select * from carbonTable where country='' and salary='')") {
    checkAnswer(
      sql("select * from carbonTable where country='' and salary=''"),
      sql("select * from hiveTable where country='' and salary=''")
    )
  }

  test("select * from carbonTable where date='' and salary='')") {
    checkAnswer(
      sql("select * from carbonTable where date='' and salary=''"),
      sql("select * from hiveTable where date='' and salary=''")
    )
  }

  override def afterAll {
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
  }
}

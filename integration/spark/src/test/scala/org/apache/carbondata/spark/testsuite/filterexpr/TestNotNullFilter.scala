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

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
 * Test cases for testing columns having \N or \null values for non numeric columns
 */
class TestNotNullFilter extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists carbonTableNotNull")
     CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
    val csvFilePath = s"$resourcesPath/filter/notNullFilter.csv"
     sql("""
           CREATE TABLE IF NOT EXISTS carbonTableNotNull
           (ID Int, date timestamp, country String,
           name String, phonetype String, serialname char(10), salary Int)
           STORED AS carbondata
           """)
     sql(s"""
           LOAD DATA LOCAL INPATH '$csvFilePath' into table carbonTableNotNull OPTIONS('BAD_RECORDS_ACTION'='FORCE')
           """)
  }


  test("select ID from carbonTableNotNull where ID is not null") {
    checkAnswer(
      sql("select ID from carbonTableNotNull where ID is not null"),
      Seq(Row(1), Row(4), Row(5), Row(6), Row(7), Row(8), Row(9), Row(10)))
  }

  override def afterAll {
    sql("drop table if exists carbonTableNotNull")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  }
}

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

package org.apache.carbondata.spark.testsuite.directdictionary


import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for detailed query on timestamp datatypes
 */
class TimestampNoDictionaryColumnCastTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

      sql("drop table if exists timestamp_nodictionary")
    sql("drop table if exists datetype")
      sql(
        """
         CREATE TABLE IF NOT EXISTS timestamp_nodictionary
        (timestamptype timestamp) STORED AS carbondata"""
      )
      val csvFilePath = s"$resourcesPath/timestampdatafile.csv"
      sql(s"LOAD DATA LOCAL INPATH '$csvFilePath' into table timestamp_nodictionary")
//
    sql(
      """
         CREATE TABLE IF NOT EXISTS datetype
        (datetype1 date) STORED AS carbondata"""
    )
    val csvFilePath1 = s"$resourcesPath/datedatafile.csv"
    sql(s"LOAD DATA LOCAL INPATH '$csvFilePath1' into table datetype")
  }

  ignore("select count(*) from timestamp_nodictionary where timestamptype BETWEEN '2018-09-11' AND '2018-09-16'") {
    checkAnswer(
      sql("select count(*) from timestamp_nodictionary where timestamptype BETWEEN '2018-09-11' AND '2018-09-16'"),
      Seq(Row(6)
      )
    )
  }
//
  test("select count(*) from datetype where datetype1 BETWEEN '2018-09-11' AND '2018-09-16'") {
    checkAnswer(
      sql("select count(*) from datetype where datetype1 BETWEEN '2018-09-11' AND '2018-09-16'"),
      Seq(Row(6)
      )
    )
  }

  override def afterAll {
    sql("drop table timestamp_nodictionary")
    sql("drop table if exists datetype")
  }
}
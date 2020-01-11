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
  * Test Class for filter expression query on String datatypes
  */
class CountStarTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists filtertestTables")
    sql("drop table if exists filterTimestampDataType")

    sql("CREATE TABLE filterTimestampDataType (ID int, date Timestamp, country String, " +
      "name String, phonetype String, serialname String, salary int) " +
      "STORED AS carbondata"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    var csvFilePath = s"$resourcesPath/datanullmeasurecol.csv"
      sql(
      s"LOAD DATA LOCAL INPATH '" + csvFilePath + "' INTO TABLE " +
        s"filterTimestampDataType " +
        s"OPTIONS('DELIMITER'= ',', " +
        s"'FILEHEADER'= '')"
    )
  }

  test("select count ") {
    checkAnswer(
      sql("select count(*) from filterTimestampDataType where country='china'"),
      Seq(Row(2))
    )
  }

  test("explain select count star without filter") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")

    sql("explain select count(*) from filterTimestampDataType").collect()

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
        CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT)
  }

  override def afterAll {
    sql("drop table if exists filtertestTables")
    sql("drop table if exists filterTimestampDataType")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}
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
package org.apache.carbondata.integration.spark.testsuite.dataload

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
 * Test Class for data loading when there are null measures in data
 *
 */
class TestLoadDataWithEmptyArrayColumns extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("drop table if exists nest13")
    sql("""
           CREATE TABLE nest13 (imei string,age int,
           productdate timestamp,gamePointId double,
           reserved6 array<string>,mobile struct<poc:string, imsi:int>)
           STORED AS carbondata
        """)
  }

  test("test carbon table data loading when there are empty array columns in data") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
      )
    sql(
      s"""
            LOAD DATA inpath '$resourcesPath/arrayColumnEmpty.csv'
            into table nest13 options ('DELIMITER'=',', 'complex_delimiter_level_1'='/')
         """
    )
    checkAnswer(
      sql("""
             SELECT count(*) from nest13
          """),
      Seq(Row(20)))
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table nest13")

  }
}

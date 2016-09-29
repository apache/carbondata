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

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import java.sql.Timestamp

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row

class TestLoadDataWithDiffTimestampFormat extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("DROP TABLE IF EXISTS t3")
    sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, starttime Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'carbondata'
        """)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
  }
git
  test("test load data with different timestamp format") {
      sql(s"""
           LOAD DATA LOCAL INPATH './src/test/resources/timeStampFormatData1.csv' into table t3
           OPTIONS('timeformat' = 'starttime:yyyy-MM-dd HH:mm:ss')
           """)
      sql(s"""
           LOAD DATA LOCAL INPATH './src/test/resources/timeStampFormatData2.csv' into table t3
           OPTIONS('timeformat' = 'date:yyyy-MM-dd,starttime:yyyy/MM/dd HH:mm:ss')
           """)
      checkAnswer(
        sql("SELECT date FROM t3 WHERE ID = 1"), Seq(Row(Timestamp.valueOf("2015-07-23 00:00:00.0")))
      )
      checkAnswer(
        sql("SELECT starttime FROM t3 WHERE ID = 1"),
        Seq(Row(Timestamp.valueOf("2016-07-23 01:01:30.0")))
      )
      checkAnswer(
        sql("SELECT date FROM t3 WHERE ID = 18"),
        Seq(Row(Timestamp.valueOf("2015-07-25 00:00:00.0")))
      )
      checkAnswer(
        sql("SELECT starttime FROM t3 WHERE ID = 18"),
        Seq(Row(Timestamp.valueOf("2016-07-25 02:32:02.0")))
      )
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS t3")
  }
}

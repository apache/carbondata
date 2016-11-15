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
package org.apache.carbondata.spark.testsuite.createtable

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.Row

import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.scalatest.BeforeAndAfterAll

class TestTrimProperty extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("DROP TABLE IF EXISTS t3")
    sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'carbondata'
           TBLPROPERTIES("trim"="country")
        """)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
  }

  test("test load data with different timestamp format") {
    sql(s"""
           LOAD DATA LOCAL INPATH './src/test/resources/testDataForTrim.csv' into table t3
           """)
    checkAnswer(
      sql("SELECT country FROM t3 WHERE ID = 1"),
      Seq(Row("china"))
    )
    checkAnswer(
      sql("SELECT name FROM t3 WHERE ID = 1"),
      Seq(Row(" aaa1          "))
    )
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS t3")
  }
}

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

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

/**
  * Test Class for table block size
  *
  */

class TestTableLevelBlockSize extends QueryTest with BeforeAndAfterAll{

  val testData1 = s"$resourcesPath/dimSample.csv"
  val testData2 = s"$resourcesPath/source.csv"

  override def beforeAll {
    sql("DROP TABLE IF EXISTS table_blocksize1")
    sql("DROP TABLE IF EXISTS table_blocksize2")
    sql("DROP TABLE IF EXISTS table_blocksize3")
    sql("DROP TABLE IF EXISTS table_max_block_size")
  }

  test("Value test: set table level blocksize value beyong [1,2048]") {
    try {
      sql(
        """
          CREATE TABLE IF NOT EXISTS table_blocksize1
          (ID Int, date Timestamp, country String,
          name String, phonetype String, serialname String, salary Int)
          STORED AS carbondata
          TBLPROPERTIES('table_blocksize'='4096 MB')
        """)
      assert(false)
    } catch {
      case e : MalformedCarbonCommandException => {
        assert(e.getMessage.equals("Invalid table_blocksize value found: 4096, " +
            "only int value from 1 MB to 2048 MB is supported."))
      }
    }
  }

  test("Value test: set table level blocksize in not int value") {
    try {
      sql(
        """
          CREATE TABLE IF NOT EXISTS table_blocksize2
          (ID Int, date Timestamp, country String,
          name String, phonetype String, serialname String, salary Int)
          STORED AS carbondata
          TBLPROPERTIES('table_blocksize'='10Y4 MB')
        """)
      assert(false)
    } catch {
      case e : MalformedCarbonCommandException => {
        assert(e.getMessage.equals("Invalid table_blocksize value found: 10y4, " +
            "only int value from 1 MB to 2048 MB is supported."))
      }
    }
  }

  test("Function test: set table level blocksize load and agg query") {

    sql(
      """
        CREATE TABLE IF NOT EXISTS table_blocksize3
        (ID Int, date Timestamp, country String,
        name String, phonetype String, serialname String, salary Int)
        STORED AS carbondata
        TBLPROPERTIES('table_blocksize'='512 MB')
      """)

    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    sql(s"""
           LOAD DATA LOCAL INPATH '$testData2' into table table_blocksize3
           """)

    checkAnswer(
      sql("""
           SELECT country, count(salary) AS amount
           FROM table_blocksize3
           WHERE country IN ('china','france')
           GROUP BY country
          """),
      Seq(Row("china", 96), Row("france", 1))
    )

  }

  test("test block size for its max value") {
    sql(
      """
        CREATE TABLE IF NOT EXISTS table_max_block_size
        (ID Int, date Timestamp, country String,
        name String, phonetype String, serialname String, salary Int)
        STORED AS carbondata
        TBLPROPERTIES('table_blocksize'='2048 MB')
      """)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(s"""
           LOAD DATA LOCAL INPATH '$testData2' into table table_max_block_size
           """)
    checkAnswer(
      sql("""
           SELECT country, count(salary) AS amount
           FROM table_max_block_size
           WHERE country IN ('china','france')
           GROUP BY country
          """),
      Seq(Row("china", 96), Row("france", 1))
    )
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS table_blocksize1")
    sql("DROP TABLE IF EXISTS table_blocksize2")
    sql("DROP TABLE IF EXISTS table_blocksize3")
    sql("DROP TABLE IF EXISTS table_max_block_size")
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }

}

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

/**
  * Test Class for load_min_size
  *
  */

class TestTableLoadMinSize extends QueryTest with BeforeAndAfterAll {
  val testData1 = s"$resourcesPath/source.csv"

  override def beforeAll {
    sql("DROP TABLE IF EXISTS table_loadminsize1")
    sql("DROP TABLE IF EXISTS table_loadminsize2")
    sql("DROP TABLE IF EXISTS table_loadminsize3")
    sql("DROP TABLE IF EXISTS table_loadminsize4")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
  }

  test("Value test: set table load min size in not int value") {
    sql(
      """
        CREATE TABLE IF NOT EXISTS table_loadminsize1
        (ID Int, date Timestamp, country String,
        name String, phonetype String, serialname String, salary Int)
        STORED AS carbondata
        TBLPROPERTIES('table_blocksize'='128 MB')
      """)

    sql(s"""
           LOAD DATA LOCAL INPATH '$testData1' into table table_loadminsize1 OPTIONS('load_min_size_inmb'='256 MB')
           """)

    checkAnswer(
      sql("""
           SELECT country, count(salary) AS amount
           FROM table_loadminsize1
           WHERE country IN ('china','france')
           GROUP BY country
          """),
      Seq(Row("china", 96), Row("france", 1))
    )
  }

  test("Function test:: set table load min size in int value") {

    sql(
      """
        CREATE TABLE IF NOT EXISTS table_loadminsize2
        (ID Int, date Timestamp, country String,
        name String, phonetype String, serialname String, salary Int)
        STORED AS carbondata
        TBLPROPERTIES('table_blocksize'='128 MB')
      """)

    sql(s"""
           LOAD DATA LOCAL INPATH '$testData1' into table table_loadminsize2 OPTIONS('load_min_size_inmb'='256')
           """)

    checkAnswer(
      sql("""
           SELECT country, count(salary) AS amount
           FROM table_loadminsize2
           WHERE country IN ('china','france')
           GROUP BY country
          """),
      Seq(Row("china", 96), Row("france", 1))
    )

  }

  test("Function test:: not set table load min size property") {

    sql(
      """
        CREATE TABLE IF NOT EXISTS table_loadminsize3
        (ID Int, date Timestamp, country String,
        name String, phonetype String, serialname String, salary Int)
        STORED AS carbondata
        TBLPROPERTIES('table_blocksize'='128 MB')
      """)

    sql(s"""
           LOAD DATA LOCAL INPATH '$testData1' into table table_loadminsize3
           """)

    checkAnswer(
      sql("""
           SELECT country, count(salary) AS amount
           FROM table_loadminsize3
           WHERE country IN ('china','france')
           GROUP BY country
          """),
      Seq(Row("china", 96), Row("france", 1))
    )

  }

  test("Function test:: set load_min_size_inmb to table property") {

    sql(
      """
        CREATE TABLE IF NOT EXISTS table_loadminsize4
        (ID Int, date Timestamp, country String,
        name String, phonetype String, serialname String, salary Int)
        STORED AS carbondata
        TBLPROPERTIES('table_blocksize'='128 MB', 'load_min_size_inmb'='256')
      """)

    sql(
      """
        desc formatted table_loadminsize4
      """).show(false)

    sql(
      """
        alter table table_loadminsize4 set TBLPROPERTIES('load_min_size_inmb'='512')
      """).show(false)

    sql(s"""
           LOAD DATA LOCAL INPATH '$testData1' into table table_loadminsize4
           """)

    checkAnswer(
      sql("""
           SELECT country, count(salary) AS amount
           FROM table_loadminsize4
           WHERE country IN ('china','france')
           GROUP BY country
          """),
      Seq(Row("china", 96), Row("france", 1))
    )

  }


  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("DROP TABLE IF EXISTS table_loadminsize1")
    sql("DROP TABLE IF EXISTS table_loadminsize2")
    sql("DROP TABLE IF EXISTS table_loadminsize3")
    sql("DROP TABLE IF EXISTS table_loadminsize4")
  }
}

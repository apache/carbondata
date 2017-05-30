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
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

/**
 * Test Class for no inverted index load and query
 *
 */

class TestNoInvertedIndexLoadAndQuery extends QueryTest with BeforeAndAfterAll {

  val testData1 = s"$resourcesPath/dimSample.csv"
  val testData2 = s"$resourcesPath/source.csv"

  override def beforeAll {
    sql("DROP TABLE IF EXISTS index1")
    sql("DROP TABLE IF EXISTS index2")
  }

  test("no inverted index load and point query") {

    sql(
      """
           CREATE TABLE IF NOT EXISTS index1
           (id Int, name String, city String)
           STORED BY 'org.apache.carbondata.format'
           TBLPROPERTIES('NO_INVERTED_INDEX'='name,city')
      """)
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$testData1' into table index1
           """)
    checkAnswer(
      sql(
        """
           SELECT * FROM index1 WHERE city = "Bangalore"
        """),
      Seq(Row(19.0, "Emily", "Bangalore")))

  }

  test("no inverted index load and agg query") {

    sql(
      """
        CREATE TABLE IF NOT EXISTS index2
        (ID Int, date Timestamp, country String,
        name String, phonetype String, serialname String, salary Int)
        STORED BY 'org.apache.carbondata.format'
        TBLPROPERTIES('NO_INVERTED_INDEX'='country,name,phonetype')
      """)

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    sql(
      s"""
           LOAD DATA LOCAL INPATH '$testData2' into table index2
           """)

    checkAnswer(
      sql(
        """
           SELECT country, count(salary) AS amount
           FROM index2
           WHERE country IN ('china','france')
           GROUP BY country
        """),
      Seq(Row("china", 96), Row("france", 1))
    )

  }

  test("no inverted index with measure") {
    sql("drop table if exists index2")

      sql(
        """

        CREATE TABLE IF NOT EXISTS index2
        (ID Int, date Timestamp, country String,
        name String, phonetype String, serialname String, salary Int)
        STORED BY 'org.apache.carbondata.format'
        TBLPROPERTIES('NO_INVERTED_INDEX'='ID')
        """)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    sql(
      s"""
           LOAD DATA LOCAL INPATH '$testData2' into table index2
           """)

    checkAnswer(
      sql(
        """
           SELECT country, count(salary) AS amount
           FROM index2
           WHERE country IN ('china','france')
           GROUP BY country
        """),
      Seq(Row("china", 96), Row("france", 1))
    )

  }


  test("no inverted index with measure as dictionary_include") {
    sql("drop table if exists index2")

    sql(
      """
        CREATE TABLE IF NOT EXISTS index2
        (ID Int, date Timestamp, country String,
        name String, phonetype String, serialname String, salary Int)
        STORED BY 'org.apache.carbondata.format'
        TBLPROPERTIES('DICTIONARY_INCLUDE'='ID','NO_INVERTED_INDEX'='ID')
      """)

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    sql(
      s"""
           LOAD DATA LOCAL INPATH '$testData2' into table index2
           """)

    checkAnswer(
      sql(
        """
           SELECT country, count(salary) AS amount
           FROM index2
           WHERE country IN ('china','france')
           GROUP BY country
        """),
      Seq(Row("china", 96), Row("france", 1))
    )

  }


  test("no inverted index with measure as sort_column") {
    sql("drop table if exists index2")
    sql(
      """
        CREATE TABLE IF NOT EXISTS index2
        (ID Int, date Timestamp, country String,
        name String, phonetype String, serialname String, salary Int)
        STORED BY 'org.apache.carbondata.format'
        TBLPROPERTIES('sort_columns'='ID','NO_INVERTED_INDEX'='ID')
      """)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    sql(
      s"""
           LOAD DATA LOCAL INPATH '$testData2' into table index2
           """)
    checkAnswer(
      sql(
        """
           SELECT country, count(salary) AS amount
           FROM index2
           WHERE country IN ('china','france')
           GROUP BY country
        """),
      Seq(Row("china", 96), Row("france", 1))
    )

  }

  test("no inverted index with Dictionary_EXCLUDE and NO_INVERTED_INDEX") {
    sql("drop table if exists index1")
    sql(
      """
           CREATE TABLE IF NOT EXISTS index1
           (id Int, name String, city String)
           STORED BY 'org.apache.carbondata.format'
           TBLPROPERTIES('DICTIONARY_EXCLUDE'='city','NO_INVERTED_INDEX'='city')
      """)
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$testData1' into table index1
           """)
    checkAnswer(
      sql(
        """
           SELECT * FROM index1 WHERE city = "Bangalore"
        """),
      Seq(Row(19.0, "Emily", "Bangalore")))
  }


  override def afterAll {
    sql("drop table index1")
    sql("drop table index2")
  }

}

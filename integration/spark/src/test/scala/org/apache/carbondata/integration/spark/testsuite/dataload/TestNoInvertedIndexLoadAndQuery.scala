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

import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.encoder.Encoding

/**
 * Test Class for no inverted index load and query
 *
 */

class TestNoInvertedIndexLoadAndQuery extends QueryTest with BeforeAndAfterAll {

  val testData1 = s"$resourcesPath/dimSample.csv"
  val testData2 = s"$resourcesPath/source.csv"

  override def beforeAll {
    clean
    sql("""
           CREATE TABLE hiveNoInvertedIndexTable
           (id Int, name String, city String) row format delimited fields terminated by ','
        """)
    sql(s"""
           LOAD DATA LOCAL INPATH '$testData1' into table hiveNoInvertedIndexTable
           """)
  }

  def clean = {
    sql("DROP TABLE IF EXISTS index1")
    sql("DROP TABLE IF EXISTS index2")
    sql("DROP TABLE IF EXISTS hiveNoInvertedIndexTable")
    sql("DROP TABLE IF EXISTS carbonNoInvertedIndexTable")
    sql("DROP TABLE IF EXISTS testNull")
  }

  test("no inverted index load and point query") {

    sql(
      """
           CREATE TABLE IF NOT EXISTS index1
           (id Int, name String, city String)
           STORED AS carbondata
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
        STORED AS carbondata
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
        STORED AS carbondata
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

  test("no inverted index with measure as sort_column") {
    sql("drop table if exists index2")
    sql(
      """
        CREATE TABLE IF NOT EXISTS index2
        (ID Int, date Timestamp, country String,
        name String, phonetype String, serialname String, salary Int)
        STORED AS carbondata
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

  test("no inverted index with NO_INVERTED_INDEX") {
    sql("drop table if exists index1")
    sql(
      """
           CREATE TABLE IF NOT EXISTS index1
           (id Int, name String, city String)
           STORED AS carbondata
           TBLPROPERTIES('NO_INVERTED_INDEX'='city')
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

  test("no inverted index test for row level filter queries") {
    sql("""
           CREATE TABLE IF NOT EXISTS carbonNoInvertedIndexTable
           (id Int, name String, city String)
           STORED AS carbondata
           TBLPROPERTIES('NO_INVERTED_INDEX'='name,city')
        """)
    sql(s"""
           LOAD DATA LOCAL INPATH '$testData1' into table carbonNoInvertedIndexTable
           OPTIONS('FILEHEADER'='id,name,city', 'BAD_RECORDS_ACTION'='FORCE')
           """)
    // row level filter evaluation test
    checkAnswer(
      sql("SELECT * FROM hiveNoInvertedIndexTable WHERE city <= 'Shanghai'"),
      sql("SELECT * FROM carbonNoInvertedIndexTable WHERE city <= 'Shanghai'"))
    checkAnswer(
      sql("SELECT * FROM hiveNoInvertedIndexTable WHERE city >= 'Shanghai'"),
      sql("SELECT * FROM carbonNoInvertedIndexTable WHERE city >= 'Shanghai'"))
    checkAnswer(
      sql("SELECT * FROM hiveNoInvertedIndexTable WHERE city < 'Shanghai'"),
      sql("SELECT * FROM carbonNoInvertedIndexTable WHERE city < 'Shanghai'"))
    checkAnswer(
      sql("SELECT * FROM hiveNoInvertedIndexTable WHERE city > 'Shanghai'"),
      sql("SELECT * FROM carbonNoInvertedIndexTable WHERE city > 'Shanghai'"))
    // range filter test
    checkAnswer(
      sql("SELECT * FROM hiveNoInvertedIndexTable WHERE city > 'Shanghai' and city < 'Washington'"),
      sql("SELECT * FROM carbonNoInvertedIndexTable WHERE city > 'Shanghai' and city < 'Washington'"))
    checkAnswer(
      sql("SELECT * FROM hiveNoInvertedIndexTable WHERE city >= 'Shanghai' and city < 'Washington'"),
      sql("SELECT * FROM carbonNoInvertedIndexTable WHERE city >= 'Shanghai' and city < 'Washington'"))
    checkAnswer(
      sql("SELECT * FROM hiveNoInvertedIndexTable WHERE city > 'Shanghai' and city <= 'Washington'"),
      sql("SELECT * FROM carbonNoInvertedIndexTable WHERE city > 'Shanghai' and city <= 'Washington'"))
  }

  test("no inverted index with describe formatted query") {
    sql("drop table if exists indexFormat")
    sql(
      """
           CREATE TABLE IF NOT EXISTS indexFormat
           (id Int, name String, city String)
           STORED AS carbondata
           TBLPROPERTIES('NO_INVERTED_INDEX'='city')
      """)
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$testData1' into table indexFormat
           """)
    checkExistence(
      sql(
        """
           describe formatted indexFormat
        """),
      true,"Inverted Index Columns")

    sql(
      """
           describe formatted indexFormat
        """).show(100, false)
  }

  test("filter query on dictionary and no inverted index column where all values are null"){
    sql("""create table testNull (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata TBLPROPERTIES('NO_INVERTED_INDEX'='C2')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table testNull OPTIONS('delimiter'=';','fileheader'='c1,c2,c3,c5')""")
    sql("""select c2 from testNull where c2 is null""").show()
    checkAnswer(sql("""select c2 from testNull where c2 is null"""), Seq(Row(null), Row(null), Row(null), Row(null), Row(null), Row(null)))
  }

  test("inverted index with measure column in INVERTED_INDEX") {
    sql("drop table if exists index1")
    sql(
      """
           CREATE TABLE IF NOT EXISTS index1
           (id Int, name String, city String)
           STORED AS carbondata
           TBLPROPERTIES('INVERTED_INDEX'='city,name,id','SORT_COLUMNS'='city,name,id')
      """)
    val carbonTable = CarbonEnv.getCarbonTable(Some("default"), "index1")(sqlContext.sparkSession)
    assert(carbonTable.getColumnByName("city").getColumnSchema.getEncodingList
      .contains(Encoding.INVERTED_INDEX))
    assert(carbonTable.getColumnByName("name").getColumnSchema.getEncodingList
      .contains(Encoding.INVERTED_INDEX))
    assert(carbonTable.getColumnByName("id").getColumnSchema.getEncodingList
      .contains(Encoding.INVERTED_INDEX))
  }

  test("test same column configured in inverted and no inverted index"){
    sql("drop table if exists index1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
           CREATE TABLE IF NOT EXISTS index1
           (id Int, name String, city String)
           STORED AS carbondata
           TBLPROPERTIES('NO_INVERTED_INDEX'='city','INVERTED_INDEX'='city','SORT_COLUMNS'='city')
      """)
    }
    assert(exception.getMessage
      .contains(
        "Column ambiguity as duplicate column(s):city is present in INVERTED_INDEX and " +
        "NO_INVERTED_INDEX. Duplicate columns are not allowed."))
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists index1")
    sql("drop table if exists index2")
    sql("drop table if exists indexFormat")
    sql("drop table if exists testNull")
    clean
  }

}

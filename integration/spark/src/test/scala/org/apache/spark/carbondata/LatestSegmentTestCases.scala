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

package org.apache.spark.carbondata

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll


class LatestSegmentTestCases extends QueryTest with BeforeAndAfterAll {
  val path = s"$resourcesPath/latest-table-data.csv"
  val path1 = s"$resourcesPath/latest-table-data1.csv"

  override def beforeAll {
    sql("DROP TABLE IF EXISTS latest_table_latest_segment")
    sql("DROP TABLE IF EXISTS latest_table")
    sql(
      s"""
         | CREATE TABLE latest_table_latest_segment(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | floatField FLOAT
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('QUERY_LATEST_SEGMENT'='true')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE latest_table(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | floatField FLOAT
         | )
         | STORED AS carbondata
       """.stripMargin)
  }

  test("test latest segment query for load") {

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path1'
         | INTO TABLE latest_table_latest_segment
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path1'
         | INTO TABLE latest_table_latest_segment
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path1'
         | INTO TABLE latest_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path1'
         | INTO TABLE latest_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

    val last_segment_result = sql(
      s"""
         | SELECT *
         | FROM latest_table_latest_segment
         | WHERE stringField = 'spark'
      """.stripMargin).collect().length

    val result = sql(
      s"""
         | SELECT *
         | FROM latest_table
         | WHERE stringField = 'spark'
      """.stripMargin).collect().length

    assert(last_segment_result == 6)
    assert(result == 12)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE latest_table_latest_segment
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

    val last_segment_result_2 = sql(
      s"""
         | SELECT *
         | FROM latest_table_latest_segment
         | WHERE intField = 10
      """.stripMargin).collect().length

    val last_segment_result_3 = sql(
      s"""
         | SELECT *
         | FROM latest_table_latest_segment
         | WHERE stringField = 'spark'
      """.stripMargin).collect().length
    assert(last_segment_result_2 == 5)
    assert(last_segment_result_3 == 4)
  }

  test("test latest segment query for load, set carbon.input.segments") {
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path1'
         | INTO TABLE latest_table_latest_segment
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path1'
         | INTO TABLE latest_table_latest_segment
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

    val last_segment_result = sql(
      s"""
         | SELECT *
         | FROM latest_table_latest_segment
         | WHERE stringField = 'spark'
      """.stripMargin).collect().length
    assert(last_segment_result == 6)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE latest_table_latest_segment
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

    sql("SET carbon.input.segments.default.latest_table_latest_segment = 0,1")
    val last_segment_result_2 = sql(
      s"""
         | SELECT *
         | FROM latest_table_latest_segment
         | WHERE intField = 10
      """.stripMargin).collect().length
    assert(last_segment_result_2 == 6)

    sql("SET carbon.input.segments.default.latest_table_latest_segment = *")
    val last_segment_result_3 = sql(
      s"""
         | SELECT *
         | FROM latest_table_latest_segment
         | WHERE stringField = 'spark'
      """.stripMargin).collect().length
    sqlContext.sparkSession.conf.unset("carbon.input.segments.default.latest_table_latest_segment")
    assert(last_segment_result_3 == 4)
  }


  test("test latest segment query for partition table ") {
    sql("DROP TABLE IF EXISTS latest_table_latest_segment")
    sql("DROP TABLE IF EXISTS latest_table")
    sql(
      s"""
         | CREATE TABLE latest_table_latest_segment(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | floatField FLOAT
         | )
         | partitioned by (dateField DATE)
         | STORED AS carbondata
         | TBLPROPERTIES('QUERY_LATEST_SEGMENT'='true')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE latest_table(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | floatField FLOAT
         | )
         | STORED AS carbondata
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path1'
         | INTO TABLE latest_table_latest_segment
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path1'
         | INTO TABLE latest_table_latest_segment
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path1'
         | INTO TABLE latest_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path1'
         | INTO TABLE latest_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)


    val last_segment_result = sql(
      s"""
         | SELECT *
         | FROM latest_table_latest_segment
         | WHERE stringField = 'spark'
      """.stripMargin).collect().length

    val result = sql(
      s"""
         | SELECT *
         | FROM latest_table
         | WHERE stringField = 'spark'
      """.stripMargin).collect().length

    assert(last_segment_result == 6)
    assert(result == 12)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE latest_table_latest_segment
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

    val last_segment_result_2 = sql(
      s"""
         | SELECT *
         | FROM latest_table_latest_segment
         | WHERE intField = 10
      """.stripMargin).collect().length

    val last_segment_result_3 = sql(
      s"""
         | SELECT *
         | FROM latest_table_latest_segment
         | WHERE stringField = 'spark'
      """.stripMargin).collect().length
    assert(last_segment_result_2 == 5)
    assert(last_segment_result_3 == 4)
  }

  test("test latest segment query for partition table set carbon.input.segments") {
    sql("DROP TABLE IF EXISTS latest_table_latest_segment")
    sql("DROP TABLE IF EXISTS latest_table")
    sql(
      s"""
         | CREATE TABLE latest_table_latest_segment(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | floatField FLOAT
         | )
         | partitioned by (dateField DATE)
         | STORED AS carbondata
         | TBLPROPERTIES('QUERY_LATEST_SEGMENT'='true')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE latest_table(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | floatField FLOAT
         | )
         | STORED AS carbondata
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path1'
         | INTO TABLE latest_table_latest_segment
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path1'
         | INTO TABLE latest_table_latest_segment
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path1'
         | INTO TABLE latest_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path1'
         | INTO TABLE latest_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)


    val last_segment_result = sql(
      s"""
         | SELECT *
         | FROM latest_table_latest_segment
         | WHERE stringField = 'spark'
      """.stripMargin).collect().length

    val result = sql(
      s"""
         | SELECT *
         | FROM latest_table
         | WHERE stringField = 'spark'
      """.stripMargin).collect().length

    assert(last_segment_result == 6)
    assert(result == 12)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE latest_table_latest_segment
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

    sql("SET carbon.input.segments.default.latest_table_latest_segment = 0,1")

    val last_segment_result_2 = sql(
      s"""
         | SELECT *
         | FROM latest_table_latest_segment
         | WHERE intField = 10
      """.stripMargin).collect().length
    assert(last_segment_result_2 == 6)

    sql("SET carbon.input.segments.default.latest_table_latest_segment = *")
    val last_segment_result_3 = sql(
      s"""
         | SELECT *
         | FROM latest_table_latest_segment
         | WHERE stringField = 'spark'
      """.stripMargin).collect().length
    assert(last_segment_result_3 == 4)
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS latest_table_latest_segment")
    sql("DROP TABLE IF EXISTS latest_table")
    sqlContext.sparkSession.conf.unset("carbon.input.segments.default.carbon_table")
  }
}

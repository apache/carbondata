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

package org.apache.carbondata.integration.spark.testsuite.preaggregate

import org.apache.spark.sql.Row
import org.apache.spark.util.SparkUtil4Test
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.util.SparkQueryTest

class TestPreAggregateIUDSuite extends SparkQueryTest with BeforeAndAfterAll with BeforeAndAfterEach {

  val testData = s"$resourcesPath/sample.csv"
  val p1 = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
      CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)

    SparkUtil4Test.createTaskMockUp(sqlContext)
    sql("DROP TABLE IF EXISTS maintable")
  }

  override protected def afterAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
        CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, p1)
    sql("DROP TABLE IF EXISTS y ")
    sql("DROP TABLE IF EXISTS maintable")
    sql("DROP TABLE IF EXISTS maintbl")
    sql("DROP TABLE IF EXISTS main_table")
  }

  override protected def beforeEach(): Unit = {
    sql("DROP TABLE IF EXISTS main_table")
    sql("DROP TABLE IF EXISTS segmaintable")
  }

  test("test pre-aggregate IUD function 2: don't support update after create pre aggregate table") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sql("reset")
    sql("DROP TABLE IF EXISTS main_table")
    sql(
      """
        | CREATE TABLE main_table(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"INSERT INTO main_table VALUES(1, 'xyz', 'bengaluru', 26)")

    sql(
      s"""
         | CREATE DATAMAP preagg_sum
         | ON TABLE main_table
         | USING 'preaggregate'
         | AS SELECT id, SUM(age)
         | FROM main_table
         | GROUP BY id
       """.stripMargin)

    val e = intercept[UnsupportedOperationException] {
      sql("UPDATE main_table SET (age) = (age + 9) WHERE name = 'bengaluru'").show()
    }

    assert(e.getMessage.equals(
      "Update operation is not supported for tables which have a pre-aggregate table. " +
        "Drop pre-aggregate tables to continue."))
  }

  test("test pre-aggregate IUD function 3: don't support delete main table column after create pre-aggregate table") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sql("reset")
    sql("DROP TABLE IF EXISTS main_table")
    sql(
      """
        | CREATE TABLE main_table(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"INSERT INTO main_table VALUES(1, 'xyz', 'bengaluru', 26)")

    sql(
      s"""
         | CREATE DATAMAP preagg_sum
         | ON TABLE main_table
         | USING 'preaggregate'
         | AS SELECT id, SUM(age)
         | FROM main_table
         | GROUP BY id
       """.stripMargin)

    val e = intercept[UnsupportedOperationException] {
      sql("DELETE FROM main_table WHERE name = 'bengaluru'").show()
    }
    assert(e.getMessage.equals(
      "Delete operation is not supported for tables which have a pre-aggregate table. " +
        "Drop pre-aggregate tables to continue."))
  }

  test("test pre-aggregate IUD function 4: don't support delete segment after create pre aggregate table") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sql("reset")
    sql("DROP TABLE IF EXISTS main_table")
    sql(
      """
        | CREATE TABLE main_table(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"INSERT INTO main_table VALUES(1, 'xyz', 'bengaluru', 26)")
    sql(s"INSERT INTO main_table VALUES(2, 'bob', 'shenzhen', 27)")
    sql(
      s"""
         | CREATE DATAMAP preagg_sum
         | ON TABLE main_table
         | USING 'preaggregate'
         | AS SELECT id, SUM(age)
         | FROM main_table
         | GROUP BY id
       """.stripMargin)

    val e = intercept[UnsupportedOperationException] {
      sql(s"""DELETE FROM TABLE main_table WHERE segment.id IN (0)""").collect
    }
    assert(e.getMessage.equals(
      "Delete segment operation is not supported on tables which have a pre-aggregate table"))

    val mainTableSegment = sql("SHOW SEGMENTS FOR TABLE main_table")
    val SegmentSequenceIds = mainTableSegment.collect().map { each => (each.toSeq) (0) }
    assert(!SegmentSequenceIds.contains(0))
    assert(!SegmentSequenceIds.contains(1))
  }

}

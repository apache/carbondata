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
package org.apache.carbondata.spark.testsuite.deletesegment

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * test class for testing the delete segment expect remaining number.
 */
class DeleteSegmentByRemainNumberTestCase extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {
  val DELETED_STATUS = "Marked for Delete"

  val SUCCESSFUL_STATUS = "Success"

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd-MM-yyyy")
  }

  override def beforeEach(): Unit = {
    initTestTable
  }

  test("delete segment, remain_number = 1") {
    sql("delete from table deleteSegmentTable expect segment.remain_number = 1")
    val rows = sql("show segments on deleteSegmentTable").collect()
    assertResult(SUCCESSFUL_STATUS)(rows(0).get(1))
    assertResult(DELETED_STATUS)(rows(1).get(1))
    assertResult(DELETED_STATUS)(rows(2).get(1))
  }

  test("delete segment, remain nothing") {
    sql("delete from table deleteSegmentTable expect segment.remain_number = 0")
    val rows = sql("show segments on deleteSegmentTable").collect()
    rows.foreach(row => assertResult(DELETED_STATUS)(row.get(1)))
  }

  test("delete segment, remain all") {
    sql("delete from table deleteSegmentTable expect segment.remain_number = 3")
    val rows = sql("show segments on deleteSegmentTable").collect()
    rows.foreach(row => assertResult(SUCCESSFUL_STATUS)(row.get(1)))
  }

  test("delete segment, remain_number = -1") {
    val ex = intercept[Exception] {
      sql("delete from table deleteSegmentTable expect segment.remain_number = -1")
    }
    assert(ex.getMessage.contains("not found"))
  }

  test("delete segment after update") {
    sql("update deleteSegmentTable d set (d.country) = ('fr') where d.country = 'aus'")
    sql("delete from table deleteSegmentTable expect segment.remain_number = 1")
    val rows = sql("select * from deleteSegmentTable").collect()
    rows.foreach(row => assertResult("fr")(row(2)))
  }

  test("delete segment after delete newest segment by segmentId") {
    sql("delete from table deleteSegmentTable where segment.id in (2)")
    sql("delete from table deleteSegmentTable expect segment.remain_number = 1")
    val rows = sql("show segments on deleteSegmentTable").collect()
    assertResult(DELETED_STATUS)(rows(0).get(1))
    assertResult(SUCCESSFUL_STATUS)(rows(1).get(1))
    assertResult(DELETED_STATUS)(rows(2).get(1))
  }

  private def initTestTable = {
    sql("drop table if exists deleteSegmentTable")
    sql(
      "CREATE table deleteSegmentTable (ID int, date String, country String, name " +
        "String, phonetype String, serialname String, salary String) STORED AS carbondata " +
        "PARTITIONED by(age int)"
    )
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention1.csv'
         | INTO TABLE deleteSegmentTable PARTITION (age='20') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention2.csv'
         | INTO TABLE deleteSegmentTable PARTITION (age='30') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention3.csv'
         | INTO TABLE deleteSegmentTable PARTITION (age='40') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
  }
}
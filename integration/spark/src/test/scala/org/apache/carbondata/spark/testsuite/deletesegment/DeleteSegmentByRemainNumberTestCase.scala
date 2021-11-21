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
class DeleteSegmentByRemainNumberTestCase extends QueryTest with BeforeAndAfterAll
  with BeforeAndAfterEach {
  val DELETED_STATUS = "Marked for Delete"

  val SUCCESSFUL_STATUS = "Success"

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd-MM-yyyy")
  }

  override def beforeEach(): Unit = {
    sql("drop table if exists deleteSegmentPartitionTable")
    sql("drop table if exists deleteSegmentTable")
    sql("drop table if exists indexTable")
    sql(
      "CREATE table deleteSegmentPartitionTable (ID int, date String, country String, name " +
        "String, phonetype String, serialname String, salary String) STORED AS carbondata " +
        "PARTITIONED by(age int)"
    )
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention1.csv'
         | INTO TABLE deleteSegmentPartitionTable PARTITION (age='20')
         | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention2.csv'
         | INTO TABLE deleteSegmentPartitionTable PARTITION (age='30')
         | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention3.csv'
         | INTO TABLE deleteSegmentPartitionTable PARTITION (age='20')
         | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention3.csv'
         | INTO TABLE deleteSegmentPartitionTable PARTITION (age='30')
         | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)

    sql(
      "CREATE table deleteSegmentTable (ID int, date String, country String, name " +
        "String, phonetype String, serialname String, salary String) STORED AS carbondata"
    )
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention1.csv'
         | INTO TABLE deleteSegmentTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention2.csv'
         | INTO TABLE deleteSegmentTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention3.csv'
         | INTO TABLE deleteSegmentTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)

    sql("create index indexTable on table deleteSegmentTable(country) as 'carbondata'" +
      "properties('sort_scope'='global_sort', 'Global_sort_partitions'='3')")
  }

  override def afterAll(): Unit = {
    sql("drop table if exists deleteSegmentTable")
    sql("drop table if exists deleteSegmentPartitionTable")
    sql("drop table if exists indexTable")
  }

  test("delete segment, remain_number = 1") {
    sql("delete from table deleteSegmentTable expect segment.remain_number = 1")
    val segments1 = sql("show segments on deleteSegmentTable").collect()
    assertResult(SUCCESSFUL_STATUS)(segments1(0).get(1))
    assertResult(DELETED_STATUS)(segments1(1).get(1))
    assertResult(DELETED_STATUS)(segments1(2).get(1))
    assertResult(sql("select * from indexTable").count())(10)

    sql("delete from table deleteSegmentPartitionTable expect segment.remain_number = 1")
    val segments2 = sql("show segments on deleteSegmentPartitionTable").collect()
    assertResult(SUCCESSFUL_STATUS)(segments2(0).get(1))
    assertResult(SUCCESSFUL_STATUS)(segments2(1).get(1))
    assertResult(DELETED_STATUS)(segments2(2).get(1))
    assertResult(DELETED_STATUS)(segments2(3).get(1))
  }

  test("delete segment, remain nothing") {
    sql("delete from table deleteSegmentTable expect segment.remain_number = 0")
    val segments1 = sql("show segments on deleteSegmentTable").collect()
    segments1.foreach(row => assertResult(DELETED_STATUS)(row.get(1)))
    assertResult(sql("select * from indexTable").count())(0)

    sql("delete from table deleteSegmentPartitionTable expect segment.remain_number = 0")
    val segments2 = sql("show segments on deleteSegmentPartitionTable").collect()
    segments2.foreach(row => assertResult(DELETED_STATUS)(row.get(1)))
  }

  test("delete segment, remain all") {
    sql("delete from table deleteSegmentTable expect segment.remain_number = 3")
    val segments1 = sql("show segments on deleteSegmentTable").collect()
    segments1.foreach(row => assertResult(SUCCESSFUL_STATUS)(row.get(1)))
    assertResult(sql("select * from indexTable").count())(30)

    sql("delete from table deleteSegmentPartitionTable expect segment.remain_number = 3")
    val segments2 = sql("show segments on deleteSegmentPartitionTable").collect()
    segments2.foreach(row => assertResult(SUCCESSFUL_STATUS)(row.get(1)))
  }

  test("delete segment, remain_number is invalid") {
    val ex1 = intercept[Exception] {
      sql("delete from table deleteSegmentTable expect segment.remain_number = -1")
    }
    assert(ex1.getMessage.contains("not found"))
    val ex2 = intercept[Exception] {
      sql("delete from table deleteSegmentTable expect segment.remain_number = 2147483648")
    }
    assert(ex2.getMessage.contains("SqlParse"))
    assertResult(sql("select * from indexTable").count())(30)

    val ex3 = intercept[Exception] {
      sql("delete from table deleteSegmentPartitionTable expect segment.remain_number = -1")
    }
    assert(ex3.getMessage.contains("not found"))
    val ex4 = intercept[Exception] {
      sql("delete from table deleteSegmentPartitionTable expect segment.remain_number = 2147483648")
    }
    assert(ex4.getMessage.contains("SqlParse"))
  }

  test("delete segment after delete newest segment by segmentId") {
    sql("delete from table deleteSegmentTable where segment.id in (2)")
    sql("delete from table deleteSegmentTable expect segment.remain_number = 1")
    val segments1 = sql("show segments on deleteSegmentTable").collect()
    assertResult(DELETED_STATUS)(segments1(0).get(1))
    assertResult(SUCCESSFUL_STATUS)(segments1(1).get(1))
    assertResult(DELETED_STATUS)(segments1(2).get(1))
    assertResult(sql("select * from indexTable").count())(10)

    sql("delete from table deleteSegmentPartitionTable where segment.id in (2)")
    sql("delete from table deleteSegmentPartitionTable expect segment.remain_number = 1")
    sql("show segments on deleteSegmentPartitionTable").show()
    val segments2 = sql("show segments on deleteSegmentPartitionTable").collect()
    assertResult(SUCCESSFUL_STATUS)(segments2(0).get(1))
    assertResult(DELETED_STATUS)(segments2(1).get(1))
    assertResult(DELETED_STATUS)(segments2(2).get(1))
    assertResult(SUCCESSFUL_STATUS)(segments2(3).get(1))
  }

  test("delete segment by partition id") {
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention1.csv'
         | INTO TABLE deleteSegmentPartitionTable PARTITION (age='20')
         | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention2.csv'
         | INTO TABLE deleteSegmentPartitionTable PARTITION (age='20')
         | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention1.csv'
         | INTO TABLE deleteSegmentPartitionTable PARTITION (age='30')
         | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention2.csv'
         | INTO TABLE deleteSegmentPartitionTable PARTITION (age='40')
         | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/dataretention2.csv'
         | INTO TABLE deleteSegmentPartitionTable PARTITION (age='40')
         | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql("delete from table deleteSegmentPartitionTable expect segment.remain_number = 1")
    val segments = sql("show segments on deleteSegmentPartitionTable").collect()
    assertResult(SUCCESSFUL_STATUS)(segments(5).get(1))
    assertResult(SUCCESSFUL_STATUS)(segments(6).get(1))
    assertResult(SUCCESSFUL_STATUS)(segments(8).get(1))
  }
}

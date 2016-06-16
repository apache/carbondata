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

package org.carbondata.spark.testsuite.filterexpr

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import java.sql.Timestamp

/**
  * Test Class for filter expression query on String datatypes
  *
  * @author N00902756
  *
  */
class FilterProcessorTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop cube if exists filtertestTables")
    sql("drop cube if exists filtertestTablesWithDecimal")
    sql("drop cube if exists filtertestTablesWithNull")
    sql("CREATE CUBE filtertestTables DIMENSIONS (ID Integer, date Timestamp, country String, " +
      "name String, phonetype String, serialname String) " +
      "MEASURES (salary Integer) " +
      "OPTIONS (PARTITIONER [PARTITION_COUNT=1])"
    )
    sql(
      s"LOAD DATA FACT FROM './src/test/resources/dataDiff.csv' INTO CUBE filtertestTables " +
        s"OPTIONS(DELIMITER ',', " +
        s"FILEHEADER '')"
    )
    sql(
      "CREATE CUBE filtertestTablesWithDecimal DIMENSIONS (ID decimal, date Timestamp, country " +
        "String, " +
        "name String, phonetype String, serialname String) " +
        "MEASURES (salary Integer) " +
        "OPTIONS (PARTITIONER [PARTITION_COUNT=1])"
    )
    sql(
      s"LOAD DATA FACT FROM './src/test/resources/dataDiff.csv' INTO CUBE " +
        s"filtertestTablesWithDecimal " +
        s"OPTIONS(DELIMITER ',', " +
        s"FILEHEADER '')"
    )
    sql(
      "CREATE CUBE filtertestTablesWithNull DIMENSIONS (ID Integer, date Timestamp, country " +
        "String, " +
        "name String, phonetype String, serialname String) " +
        "MEASURES (salary Integer) " +
        "OPTIONS (PARTITIONER [PARTITION_COUNT=1])"
    )
    sql(
      s"LOAD DATA FACT FROM './src/test/resources/data2.csv' INTO CUBE " +
        s"filtertestTablesWithNull " +
        s"OPTIONS(DELIMITER ',', " +
        s"FILEHEADER '')"
    )
  }

  test("Is not null filter") {
    checkAnswer(
      sql("select id from filtertestTablesWithNull " + "where id is not null"),
      Seq(Row(4), Row(6))
    )
  }

  test("Greater Than Filter") {
    checkAnswer(
      sql("select id from filtertestTables " + "where id >999"),
      Seq(Row(1000))
    )
  }
  test("Greater Than Filter with decimal") {
    checkAnswer(
      sql("select id from filtertestTablesWithDecimal " + "where id >999"),
      Seq(Row(1000))
    )
  }

  test("Greater Than equal to Filter") {
    checkAnswer(
      sql("select id from filtertestTables " + "where id >=999"),
      Seq(Row(999), Row(1000))
    )
  }

  test("Greater Than equal to Filter with decimal") {
    checkAnswer(
      sql("select id from filtertestTables " + "where id >=999"),
      Seq(Row(999), Row(1000))
    )
  }
  test("Include Filter") {
    checkAnswer(
      sql("select id from filtertestTables " + "where id =999"),
      Seq(Row(999))
    )
  }
  test("In Filter") {
    checkAnswer(
      sql(
        "select Country from filtertestTables where Country in ('china','france') group by Country"
      ),
      Seq(Row("china"), Row("france"))
    )
  }

  test("Logical condition") {
    checkAnswer(
      sql("select id,country from filtertestTables " + "where country='china' and name='aaa1'"),
      Seq(Row(1, "china"))
    )
  }

  override def afterAll {
    // sql("drop cube filtertestTable")
  }
}
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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

class TestLoadOptions extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists TestLoadTableOptions")
    sql("CREATE table TestLoadTableOptions (ID int, date String, country String, name String," +
        "phonetype String, serialname String, salary int) STORED AS carbondata")
  }

  override def afterAll {
    sql("drop table if exists TestLoadTableOptions")
  }

  test("test load data with more than one char in commentchar option") {
    val errorMessage = intercept[MalformedCarbonCommandException] {
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE " +
          s"TestLoadTableOptions OPTIONS('COMMENTCHAR'='##')")
      assert(false)
    }.getMessage
    assert(errorMessage.equals("COMMENTCHAR cannot be more than one character."))
  }

  test("test load data with more than one char in escapechar option") {
    val errorMessage = intercept[MalformedCarbonCommandException] {
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE " +
          s"TestLoadTableOptions OPTIONS('ESCAPECHAR'='\\\\')")
      assert(false)
    }.getMessage
    assert(errorMessage.equals("ESCAPECHAR cannot be more than one character."))
  }

  test("test load data with invalid escape sequence in escapechar option") {
    val errorMessage = intercept[MalformedCarbonCommandException] {
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE " +
          s"TestLoadTableOptions OPTIONS('ESCAPECHAR'='\\y')")
    }.getMessage
    assert(errorMessage.equals("ESCAPECHAR cannot be more than one character."))
  }

  test("test load data with with valid escape sequence in escapechar option") {
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention11.csv' INTO TABLE " +
        s"TestLoadTableOptions OPTIONS('ESCAPECHAR'='\\n')")
    if(SPARK_VERSION.startsWith("3")) {
      val parquetTable = EscapeCharTest.createParquetTable("\n", "dataretention11.csv")
      checkAnswer(sql("select * from TestLoadTableOptions where serialname='ASD69643a'"),
        sql(s"select * from $parquetTable where serialname='ASD69643a'"))
    } else {
      checkAnswer(sql("select * from TestLoadTableOptions where serialname='ASD69643a'"),
        Row(1, "2015/7/23", "ind", "aaa1", "phone197", "ASD69643a", 15000))
    }
  }

  test("test load data with different line separator option value") {
    // line separator as '\n'
    sql("drop table if exists carriage_return_in_string")
    sql("""CREATE TABLE IF NOT EXISTS carriage_return_in_string(ID BigInt, name String,
          |city String) STORED AS carbondata""".stripMargin.replace('\n', ' '))
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/carriage_return_in_string.csv' INTO TABLE
         |carriage_return_in_string
         | OPTIONS ('fileheader'='id, name, city', 'line_separator'='\\n')"""
        .stripMargin.replace('\n', ' '));
    checkAnswer(sql("select * from carriage_return_in_string where id = 1"),
      Row(1, "2\r", "3"))

    // without line separator
    sql("drop table if exists carriage_return_in_string")
    sql("""CREATE TABLE IF NOT EXISTS carriage_return_in_string(ID BigInt, name String,
          |city String) STORED AS carbondata""".stripMargin.replace('\n', ' '))
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/carriage_return_in_string.csv' INTO TABLE
           |carriage_return_in_string OPTIONS ('fileheader'='id, name, city')"""
      .stripMargin.replace('\n', ' '));
    checkAnswer(sql("select * from carriage_return_in_string where id = 1"),
      Row(1, "2", null))

    // line separator as '\r\n'
    sql("drop table if exists carriage_return_in_string")
    sql("""CREATE TABLE IF NOT EXISTS carriage_return_in_string(ID BigInt, name String,
          |city String) STORED AS carbondata""".stripMargin.replace('\n', ' '))
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/carriage_return_in_string.csv' INTO TABLE
           |carriage_return_in_string OPTIONS ('fileheader'='id, name, city',
           |'line_separator'='\\r\\n')""".stripMargin.replace('\n', ' '));
    checkAnswer(sql("select * from carriage_return_in_string where id = 1"),
      Row(1, "2\r", "3\n4"))

    // line separator as 'ab'
    sql("drop table if exists carriage_return_in_string")
    sql("""CREATE TABLE IF NOT EXISTS carriage_return_in_string(ID BigInt, name String,
          |city String) STORED AS carbondata""".stripMargin.replace('\n', ' '))
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/carriage_return_in_string.csv' INTO TABLE
           |carriage_return_in_string OPTIONS ('fileheader'='id, name, city',
           |'line_separator'='ab')""".stripMargin.replace('\n', ' '));
    checkAnswer(sql("select * from carriage_return_in_string where id = 1"),
      Row(1, "2\r", "3\n4"))
    sql("drop table if exists carriage_return_in_string")
  }

  test("test load data with invalidated line separator option value") {
    sql("drop table if exists carriage_return_in_string")
    sql("""CREATE TABLE IF NOT EXISTS carriage_return_in_string(ID BigInt, name String,
          |city String) STORED AS carbondata""".stripMargin.replace('\n', ' '))

    // line separator as ''
    var exception = intercept[MalformedCarbonCommandException] {
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/carriage_return_in_string.csv' INTO TABLE
           |carriage_return_in_string OPTIONS ('fileheader'='id, name, city',
           |'line_separator'='')""".stripMargin.replace('\n', ' '));
    }
    assert(exception.getMessage.contains("LINE_SEPARATOR can be only one or two characters."))

    // line separator as '\r\na'
    exception = intercept[MalformedCarbonCommandException] {
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/carriage_return_in_string.csv' INTO TABLE
             |carriage_return_in_string OPTIONS ('fileheader'='id, name, city',
             |'line_separator'='\\r\\na')""".stripMargin.replace('\n', ' '));
    }
    assert(exception.getMessage.contains("LINE_SEPARATOR can be only one or two characters."))
    sql("drop table if exists carriage_return_in_string")
  }
}

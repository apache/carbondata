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

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

class TestLoadDataWithMalformedCarbonCommandException extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    sql("CREATE table TestLoadTableOptions (ID int, date String, country String, name String," +
        "phonetype String, serialname String, salary int) stored by 'org.apache.carbondata.format'")

  }

  override def afterAll {
    sql("drop table TestLoadTableOptions")
  }

  def buildTableWithNoExistDictExclude() = {
      sql(
        """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'org.apache.carbondata.format'
           TBLPROPERTIES('DICTIONARY_EXCLUDE'='country,phonetype,CCC')
        """)
  }

  def buildTableWithNoExistDictInclude() = {
      sql(
        """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'org.apache.carbondata.format'
           TBLPROPERTIES('DICTIONARY_INCLUDE'='AAA,country')
        """)
  }

  def buildTableWithSameDictExcludeAndInclude() = {
      sql(
        """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'org.apache.carbondata.format'
           TBLPROPERTIES('DICTIONARY_INCLUDE'='country','DICTIONARY_EXCLUDE'='country')
        """)
  }

  def buildTableWithSameDictExcludeAndIncludeWithSpaces() = {
    sql(
      """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'org.apache.carbondata.format'
           TBLPROPERTIES('DICTIONARY_INCLUDE'='country','DICTIONARY_EXCLUDE'='country ')
      """)
  }

  test("test load data with dictionary exclude columns which no exist in table.") {
    try {
      buildTableWithNoExistDictExclude()
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.equals("DICTIONARY_EXCLUDE column: ccc does not exist in table. " +
          "Please check create table statement."))
      case _: Throwable => assert(false)
    }
  }

  test("test load data with dictionary include columns which no exist in table.") {
    try {
      buildTableWithNoExistDictInclude()
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.equals("DICTIONARY_INCLUDE column: aaa does not exist in table. " +
          "Please check create table statement."))
      case _: Throwable => assert(false)
    }
  }

  test("test load data with dictionary include is same with dictionary exclude") {
    try {
      buildTableWithSameDictExcludeAndInclude()
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.equals("DICTIONARY_EXCLUDE can not contain the same column: country " +
          "with DICTIONARY_INCLUDE. Please check create table statement."))
      case _: Throwable => assert(false)
    }
  }

  test("test load data with invalid option") {
    try {
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE " +
        "TestLoadTableOptions OPTIONS('QUOTECHAR'='\"', 'DELIMITERRR' =  ',')")
      assert(false)
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.equals("Error: Invalid option(s): delimiterrr"))
      case _: Throwable => assert(false)
    }
  }

  test("test load data with duplicate options") {
    try {
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE " +
        "TestLoadTableOptions OPTIONS('DELIMITER' =  ',', 'quotechar'='\"', 'DELIMITER' =  '$')")
      assert(false)
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.equals("Error: Duplicate option(s): delimiter"))
      case _: Throwable => assert(false)
    }
  }

  test("test load data with case sensitive options") {
    try {
      sql(
        s"LOAD DATA local inpath '$resourcesPath/dataretention1.csv' INTO table " +
          "TestLoadTableOptions options('DeLIMITEr'=',', 'qUOtECHAR'='\"')"
      )
    } catch {
      case _: Throwable => assert(false)
    }
  }

  test("test load data with dictionary include is same with dictionary exclude with spaces") {
    try {
      buildTableWithSameDictExcludeAndIncludeWithSpaces()
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.equals("DICTIONARY_EXCLUDE can not contain the same column: country " +
          "with DICTIONARY_INCLUDE. Please check create table statement."))
      case _: Throwable => assert(false)
    }
  }
}

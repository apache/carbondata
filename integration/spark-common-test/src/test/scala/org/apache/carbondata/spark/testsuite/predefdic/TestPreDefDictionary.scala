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

package org.apache.carbondata.spark.testsuite.predefdic

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.api.CarbonProperties

/**
 * Test cases for testing columns having \N or \null values for non numeric columns
 */
class TestPreDefDictionary extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS predefdictable")
    sql("DROP TABLE IF EXISTS predefdictable1")
    sql("DROP TABLE IF EXISTS columndicTable")
    CarbonProperties.getInstance()
      .addProperty("carbon.timestamp.format",
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
      )
  }

  test("dictionary value not present in the allpredefdictionary dic file must be loaded.") {
    val csvFilePath = s"$resourcesPath/nullvalueserialization.csv"
    val testData = s"$resourcesPath/predefdic/data3.csv"
    val csvHeader = "ID,phonetype"
    val allDictFile = s"$resourcesPath/predefdic/allpredefdictionary.csv"
    sql(
      """CREATE TABLE IF NOT EXISTS predefdictable (ID Int, phonetype String)
       STORED BY 'carbondata'""")
    sql(
      s"""LOAD DATA LOCAL INPATH '$testData' into table predefdictable
           options('ALL_DICTIONARY_PATH'='$allDictFile','single_pass'='true')""")
    checkAnswer(
      sql("select phonetype from predefdictable where phonetype='phone197'"),
      Seq(Row("phone197"))
    )
  }

  test("dictionary value not present in the allpredefdictionary dic with single_pass.") {
    val csvFilePath = s"$resourcesPath/nullvalueserialization.csv"
    val testData = s"$resourcesPath/predefdic/data3.csv"
    val csvHeader = "ID,phonetype"
    val allDictFile = s"$resourcesPath/predefdic/allpredefdictionary.csv"
    sql(
      """CREATE TABLE IF NOT EXISTS predefdictable1 (ID Int, phonetype String)
       STORED BY 'carbondata'""")
    sql(
      s"""LOAD DATA LOCAL INPATH '$testData' into table predefdictable1
           options('ALL_DICTIONARY_PATH'='$allDictFile', 'SINGLE_PASS'='true')""")
    checkAnswer(
      sql("select phonetype from predefdictable1 where phonetype='phone197'"),
      Seq(Row("phone197"))
    )
  }

  test("dictionary value not present in the columndict dic with single_pass.") {
    val csvFilePath = s"$resourcesPath/nullvalueserialization.csv"
    val testData = s"$resourcesPath/predefdic/data3.csv"
    val csvHeader = "ID,phonetype"
    val dicFilePath = s"$resourcesPath/predefdic/dicfilepath.csv"
    sql(
      """CREATE TABLE IF NOT EXISTS columndicTable (ID Int, phonetype String)
       STORED BY 'carbondata'""")
    sql(
      s"""LOAD DATA LOCAL INPATH '$testData' into table columndicTable
           options('COLUMNDICT'='phonetype:$dicFilePath', 'SINGLE_PASS'='true')""")
    checkAnswer(
      sql("select phonetype from columndicTable where phonetype='phone197'"),
      Seq(Row("phone197"))
    )
  }

  test("validation test columndict with single_pass= false.") {
    val csvFilePath = s"$resourcesPath/nullvalueserialization.csv"
    val testData = s"$resourcesPath/predefdic/data3.csv"
    val csvHeader = "ID,phonetype"
    val dicFilePath = s"$resourcesPath/predefdic/dicfilepath.csv"
    sql(
      """CREATE TABLE IF NOT EXISTS columndicValidationTable (ID Int, phonetype String)
       STORED BY 'carbondata'""")
    try {
      sql(
        s"""LOAD DATA LOCAL INPATH '$testData' into table columndicValidationTable
           options('COLUMNDICT'='phonetype:$dicFilePath', 'SINGLE_PASS'='false')""")
    } catch {
      case x: Throwable =>
        val failMess: String = "Can not use all_dictionary_path or columndict without single_pass."
        assert(failMess.equals(x.getMessage))
    }
  }

  test("validation test ALL_DICTIONARY_PATH with single_pass= false.") {
    val csvFilePath = s"$resourcesPath/nullvalueserialization.csv"
    val testData = s"$resourcesPath/predefdic/data3.csv"
    val csvHeader = "ID,phonetype"
    val allDictFile = s"$resourcesPath/predefdic/allpredefdictionary.csv"
    sql(
      """CREATE TABLE IF NOT EXISTS predefdictableval (ID Int, phonetype String)
       STORED BY 'carbondata'""")
    try {
    sql(
      s"""LOAD DATA LOCAL INPATH '$testData' into table predefdictableval
           options('ALL_DICTIONARY_PATH'='$allDictFile', 'SINGLE_PASS'='false')""")
    } catch {
      case x: Throwable =>
        val failMess: String = "Can not use all_dictionary_path or columndict without single_pass."
        assert(failMess.equals(x.getMessage))
    }
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS predefdictable")
    sql("DROP TABLE IF EXISTS predefdictable1")
    sql("DROP TABLE IF EXISTS columndicTable")
    sql("DROP TABLE IF EXISTS columndicValidationTable")
    sql("DROP TABLE IF EXISTS predefdictableval")

  }
}

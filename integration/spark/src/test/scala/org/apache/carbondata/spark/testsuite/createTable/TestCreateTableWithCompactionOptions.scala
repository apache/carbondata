/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the"License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an"AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.spark.testsuite.createTable

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestCreateTableWithCompactionOptions extends QueryTest with BeforeAndAfterAll {

  val tableWithCompactionOptions = "tableWithCompactionOptions"
  val tableWithInvalidMajorCompactionSize = "tableWithInvalidMajorCompactionSize"
  val tableWithInvalidAutoLoadMerge = "tableWithInvalidAutoLoadMerge"
  val tableWithInvalidLevelThreshold = "tableWithInvalidLevelThreshold"
  val tableWithInvalidPreserveSegments = "tableWithInvalidPreserveSegments"
  val tableWithInvalidAllowedDays = "tableWithInvalidAllowedDays"
  val tableWithoutCompactionOptions = "tableWithoutCompactionOptions"

  override def beforeAll: Unit = {
    cleanTables()
  }

  override def afterAll: Unit = {
    cleanTables()
  }

  private def cleanTables(): Unit = {
    sql("use default")
    sql(s"DROP TABLE IF EXISTS $tableWithCompactionOptions")
    sql(s"DROP TABLE IF EXISTS $tableWithInvalidMajorCompactionSize")
    sql(s"DROP TABLE IF EXISTS $tableWithInvalidAutoLoadMerge")
    sql(s"DROP TABLE IF EXISTS $tableWithInvalidLevelThreshold")
    sql(s"DROP TABLE IF EXISTS $tableWithInvalidPreserveSegments")
    sql(s"DROP TABLE IF EXISTS $tableWithInvalidAllowedDays")
    sql(s"DROP TABLE IF EXISTS $tableWithoutCompactionOptions")
  }

  test("test create table with compaction options") {
    sql(
      s"""
         | CREATE TABLE $tableWithCompactionOptions(
         | intField INT,
         | stringField STRING
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('MAJOR_COMPACTION_SIZE'='10240',
         | 'AUTO_LOAD_MERGE'='true',
         | 'COMPACTION_LEVEL_THRESHOLD'='5,6',
         | 'COMPACTION_PRESERVE_SEGMENTS'='10',
         | 'ALLOWED_COMPACTION_DAYS'='5')
       """.stripMargin)

    val tableOptions = sql(s"DESCRIBE FORMATTED $tableWithCompactionOptions")
      .collect().map(r => (r.getString(0).trim, r.getString(1).trim)).toMap

    assert(tableOptions.contains("MAJOR_COMPACTION_SIZE"))
    assert(tableOptions.getOrElse("MAJOR_COMPACTION_SIZE","").equals("10240"))
    assert(tableOptions.contains("AUTO_LOAD_MERGE"))
    assert(tableOptions.getOrElse("AUTO_LOAD_MERGE","").equals("true"))
    assert(tableOptions.contains("COMPACTION_LEVEL_THRESHOLD"))
    assert(tableOptions.getOrElse("COMPACTION_LEVEL_THRESHOLD","").equals("5,6"))
    assert(tableOptions.contains("COMPACTION_PRESERVE_SEGMENTS"))
    assert(tableOptions.getOrElse("COMPACTION_PRESERVE_SEGMENTS","").equals("10"))
    assert(tableOptions.contains("ALLOWED_COMPACTION_DAYS"))
    assert(tableOptions.getOrElse("ALLOWED_COMPACTION_DAYS","").equals("5"))
  }

  test("test create table with invalid major compaction size") {
    val exception: Exception = intercept[Exception] {
      sql(
        s"""
           |CREATE TABLE $tableWithInvalidMajorCompactionSize
           |(
           |intField INT,
           |stringField STRING
           |)
           |STORED AS carbondata
           |TBLPROPERTIES('MAJOR_COMPACTION_SIZE'='abc')
       """.stripMargin)
    }
    assert(exception.getMessage.contains(
      "Invalid major_compaction_size value found: abc, " +
        "only int value greater than 0 is supported."))
  }

  test("test create table with invalid auto load merge") {
    val exception: Exception = intercept[Exception] {
      sql(
        s"""
           |CREATE TABLE $tableWithInvalidAutoLoadMerge
           |(
           |intField INT,
           |stringField STRING
           |)
           |STORED AS carbondata
           |TBLPROPERTIES('AUTO_LOAD_MERGE'='123')
       """.stripMargin)
    }
    assert(exception.getMessage.contains(
      "Invalid auto_load_merge value found: 123, only true|false is supported."))
  }

  test("test create table with invalid level threshold") {
    val exception: Exception = intercept[Exception] {
      sql(
        s"""
           |CREATE TABLE $tableWithInvalidLevelThreshold
           |(
           |intField INT,
           |stringField STRING
           |)
           |STORED AS carbondata
           |TBLPROPERTIES(
           |'AUTO_LOAD_MERGE'='true',
           |'COMPACTION_LEVEL_THRESHOLD'='x,6')
       """.stripMargin)
    }
    assert(exception.getMessage.contains(
      "Invalid compaction_level_threshold value found: x,6, " +
        "only int values separated by comma and between 0 and 100 are supported."))
  }

  test("test create table with invalid preserve segments number") {
    val exception: Exception = intercept[Exception] {
      sql(
        s"""
           |CREATE TABLE $tableWithInvalidPreserveSegments
           |(
           |intField INT,
           |stringField STRING
           |)
           |STORED AS carbondata
           |TBLPROPERTIES(
           |'AUTO_LOAD_MERGE'='true',
           |'COMPACTION_LEVEL_THRESHOLD'='4,6',
           |'COMPACTION_PRESERVE_SEGMENTS'='abc')
       """.stripMargin)
    }
    assert(exception.getMessage.contains(
      "Invalid compaction_preserve_segments value found: abc, " +
        "only int value between 0 and 100 is supported."))
  }

  test("test create table with invalid allowed days") {
    val exception: Exception = intercept[Exception] {
      sql(
        s"""
           |CREATE TABLE $tableWithInvalidAllowedDays
           |(
           |intField INT,
           |stringField STRING
           |)
           |STORED AS carbondata
           |TBLPROPERTIES(
           |'AUTO_LOAD_MERGE'='true',
           |'ALLOWED_COMPACTION_DAYS'='abc')
       """.stripMargin)
    }
    assert(exception.getMessage.contains(
      "Invalid allowed_compaction_days value found: abc, " +
        "only int value between 0 and 100 is supported."))
  }

}

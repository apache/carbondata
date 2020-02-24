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

package org.apache.carbondata.spark.testsuite.alterTable

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

/**
 * test class for validating alter table set properties with alter_column_meta_cache and
 * cache_level properties
 */
class TestAlterTableWithColumnMetCacheAndCacheLevelProperty extends QueryTest with BeforeAndAfterAll {

  private def isExpectedValueValid(dbName: String,
      tableName: String,
      key: String,
      expectedValue: String): Boolean = {
    val carbonTable = CarbonEnv.getCarbonTable(Option(dbName), tableName)(sqlContext.sparkSession)
    val value = carbonTable.getTableInfo.getFactTable.getTableProperties.get(key)
    expectedValue.equals(value)
  }

  private def dropTable = {
    sql("drop table if exists alter_column_meta_cache")
    sql("drop table if exists cache_level")
  }

  override def beforeAll {
    // drop table
    dropTable
    // create table
    sql("create table alter_column_meta_cache(c1 String, c2 String, c3 int, c4 double, c5 struct<imei:string, imsi:string>, c6 array<string>) STORED AS carbondata")
    sql("create table cache_level(c1 String) STORED AS carbondata")
  }

  test("validate column_meta_cache with only empty spaces - alter_column_meta_cache_01") {
    intercept[RuntimeException] {
      sql("Alter table alter_column_meta_cache SET TBLPROPERTIES('column_meta_cache'='    ')")
    }
  }

  test("validate the property with characters in different cases - alter_column_meta_cache_02") {
    sql("Alter table alter_column_meta_cache SET TBLPROPERTIES('COLUMN_meta_CachE'='c2,c3')")
    assert(isExpectedValueValid("default", "alter_column_meta_cache", "column_meta_cache", "c2,c3"))
  }

  test("validate column_meta_cache with intermediate empty string between columns - alter_column_meta_cache_03") {
    intercept[RuntimeException] {
      sql("Alter table alter_column_meta_cache SET TBLPROPERTIES('column_meta_cache'='c2,  ,c3')")
    }
  }

  test("validate column_meta_cache with combination of valid and invalid columns - alter_column_meta_cache_04") {
    intercept[RuntimeException] {
      sql("Alter table alter_column_meta_cache SET TBLPROPERTIES('column_meta_cache'='c2,c10')")
    }
  }

  test("validate column_meta_cache for dimensions and measures - alter_column_meta_cache_05") {
    sql("Alter table alter_column_meta_cache SET TBLPROPERTIES('column_meta_cache'='c3,c2,c4')")
    assert(isExpectedValueValid("default", "alter_column_meta_cache", "column_meta_cache", "c2,c3,c4"))
  }

  test("validate for duplicate column names - alter_column_meta_cache_06") {
    intercept[RuntimeException] {
      sql("Alter table alter_column_meta_cache SET TBLPROPERTIES('column_meta_cache'='c2,c2,c3')")
    }
  }

  test("validate column_meta_cache for complex struct type - alter_column_meta_cache_07") {
    intercept[RuntimeException] {
      sql("Alter table alter_column_meta_cache SET TBLPROPERTIES('column_meta_cache'='c5')")
    }
  }

  test("validate column_meta_cache for complex array type - alter_column_meta_cache_08") {
    intercept[RuntimeException] {
      sql("Alter table alter_column_meta_cache SET TBLPROPERTIES('column_meta_cache'='c5,c2')")
    }
  }

  test("validate column_meta_cache with empty value - alter_column_meta_cache_09") {
    sql("Alter table alter_column_meta_cache SET TBLPROPERTIES('column_meta_cache'='')")
    assert(isExpectedValueValid("default", "alter_column_meta_cache", "column_meta_cache", ""))
  }

  test("validate describe formatted command to display column_meta_cache when column_meta_cache is set - alter_column_meta_cache_10") {
    sql("Alter table alter_column_meta_cache SET TBLPROPERTIES('column_meta_cache'='c2')")
    val descResult = sql("describe formatted alter_column_meta_cache")
    checkExistence(descResult, true, "column_meta_cache")
  }

  test("validate unsetting of column_meta_cache when column_meta_cache is already set - alter_column_meta_cache_11") {
    sql("Alter table alter_column_meta_cache SET TBLPROPERTIES('column_meta_cache'='c2,c3')")
    var descResult = sql("describe formatted alter_column_meta_cache")
    checkExistence(descResult, true, "Cached Min/Max Index Columns c2, c3")
    sql("Alter table alter_column_meta_cache UNSET TBLPROPERTIES('column_meta_cache')")
    descResult = sql("describe formatted alter_column_meta_cache")
    checkExistence(descResult, false, "Cached Min/Max Index Columns c2, c3")
  }

  test("validate unsetting of column_meta_cache when column_meta_cache is not already set - alter_column_meta_cache_12") {
    var descResult = sql("describe formatted alter_column_meta_cache")
    checkExistence(descResult, false, "c2, c3")
    sql("Alter table alter_column_meta_cache UNSET TBLPROPERTIES('column_meta_cache')")
    descResult = sql("describe formatted alter_column_meta_cache")
    checkExistence(descResult, false, "c2, c3")
  }

  test("validate cache_level with only empty spaces - ALTER_CACHE_LEVEL_01") {
    intercept[RuntimeException] {
      sql("Alter table cache_level SET TBLPROPERTIES('cache_level'='    ')")
    }
  }

  test("validate cache_level with invalid values - ALTER_CACHE_LEVEL_02") {
    intercept[RuntimeException] {
      sql("Alter table cache_level SET TBLPROPERTIES('cache_level'='xyz,abc')")
    }
  }

  test("validate cache_level with property in different cases - ALTER_CACHE_LEVEL_03") {
    sql("Alter table cache_level SET TBLPROPERTIES('CACHE_leveL'='BLOcK')")
    assert(isExpectedValueValid("default", "cache_level", "cache_level", "BLOCK"))
  }

  test("validate cache_level with default value as Blocklet - ALTER_CACHE_LEVEL_04") {
    sql("Alter table cache_level SET TBLPROPERTIES('cache_level'='bloCKlet')")
    assert(isExpectedValueValid("default", "cache_level", "cache_level", "BLOCKLET"))
  }

  test("validate describe formatted command to display cache_level when cache_level is set - ALTER_CACHE_LEVEL_05") {
    sql("Alter table cache_level SET TBLPROPERTIES('cache_level'='bloCKlet')")
    val descResult = sql("describe formatted cache_level")
    checkExistence(descResult, true, "Min/Max Index Cache Level")
  }

  test("validate describe formatted command to display cache_level when cache_level is not set - ALTER_CACHE_LEVEL_06") {
    sql("Alter table cache_level UNSET TBLPROPERTIES('cache_level')")
    val descResult = sql("describe formatted cache_level")
    // even though not configured default cache level will be displayed as BLOCK
    checkExistence(descResult, true, "Min/Max Index Cache Level")
  }

  override def afterAll: Unit = {
    // drop table
    dropTable
  }

}

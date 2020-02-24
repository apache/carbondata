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

package org.apache.carbondata.spark.testsuite.createTable

import scala.collection.JavaConverters._

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
 * test class for validating create table with column_meta_cache and cache_level properties
 */
class TestCreateTableWithColumnMetCacheAndCacheLevelProperty extends QueryTest with BeforeAndAfterAll {

  private def isExpectedValueValid(dbName: String,
      tableName: String,
      key: String,
      expectedValue: String): Boolean = {
    val carbonTable = CarbonEnv.getCarbonTable(Option(dbName), tableName)(sqlContext.sparkSession)
    if (key.equalsIgnoreCase(CarbonCommonConstants.COLUMN_META_CACHE)) {
      val value = carbonTable.getMinMaxCachedColumnsInCreateOrder.asScala.mkString(",")
      expectedValue.equals(value)
    } else {
      val value = carbonTable.getTableInfo.getFactTable.getTableProperties.get(key)
      expectedValue.equals(value)
    }
  }

  test("validate column_meta_cache with only empty spaces - COLUMN_META_CACHE_01") {
    sql("drop table if exists column_meta_cache")
    intercept[MalformedCarbonCommandException] {
      sql("create table column_meta_cache(c1 String, c2 String, c3 int, c4 double) STORED AS carbondata TBLPROPERTIES('column_meta_cache'='    ')")
    }
  }

  test("validate the property with characters in different cases - COLUMN_META_CACHE_02") {
    sql("drop table if exists column_meta_cache")
    sql("create table column_meta_cache(c1 String, c2 String, c3 int, c4 double) STORED AS carbondata TBLPROPERTIES('COLUMN_meta_CachE'='c2')")
    assert(isExpectedValueValid("default", "column_meta_cache", "column_meta_cache", "c2"))
  }

  test("validate column_meta_cache with intermediate empty string between columns - COLUMN_META_CACHE_03") {
    sql("drop table if exists column_meta_cache")
    intercept[MalformedCarbonCommandException] {
      sql("create table column_meta_cache(c1 String, c2 String, c3 int, c4 double) STORED AS carbondata TBLPROPERTIES('column_meta_cache'='c2,  ,c3')")
    }
  }

  test("validate column_meta_cache with combination of valid and invalid columns - COLUMN_META_CACHE_04") {
    sql("drop table if exists column_meta_cache")
    intercept[MalformedCarbonCommandException] {
      sql("create table column_meta_cache(c1 String, c2 String, c3 int, c4 double) STORED AS carbondata TBLPROPERTIES('column_meta_cache'='c2,c10')")
    }
  }

  test("validate column_meta_cache for dimensions and measures - COLUMN_META_CACHE_05") {
    sql("drop table if exists column_meta_cache")
    sql("create table column_meta_cache(c1 String, c2 String, c3 int, c4 double) STORED AS carbondata TBLPROPERTIES('column_meta_cache'='c3,c2,c4')")
    assert(isExpectedValueValid("default", "column_meta_cache", "column_meta_cache", "c2,c3,c4"))
  }

  test("validate for duplicate column names - COLUMN_META_CACHE_06") {
    sql("drop table if exists column_meta_cache")
    intercept[MalformedCarbonCommandException] {
      sql("create table column_meta_cache(c1 String, c2 String, c3 int, c4 double) STORED AS carbondata TBLPROPERTIES('column_meta_cache'='c2,c2,c3')")
    }
  }

  test("validate column_meta_cache for complex struct type - COLUMN_META_CACHE_07") {
    sql("drop table if exists column_meta_cache")
    intercept[MalformedCarbonCommandException] {
      sql("create table column_meta_cache(c1 String, c2 String, c3 int, c4 double, c5 struct<imei:string, imsi:string>) STORED AS carbondata TBLPROPERTIES('column_meta_cache'='c5')")
    }
  }

  test("validate column_meta_cache for complex array type - COLUMN_META_CACHE_08") {
    sql("drop table if exists column_meta_cache")
    intercept[MalformedCarbonCommandException] {
      sql("create table column_meta_cache(c1 String, c2 String, c3 int, c4 double, c5 array<string>) STORED AS carbondata TBLPROPERTIES('column_meta_cache'='c5,c2')")
    }
  }

  test("validate column_meta_cache with empty value - COLUMN_META_CACHE_09") {
    sql("drop table if exists column_meta_cache")
    sql("create table column_meta_cache(c1 String, c2 String, c3 int, c4 double) STORED AS carbondata TBLPROPERTIES('column_meta_cache'='')")
    assert(isExpectedValueValid("default", "column_meta_cache", "column_meta_cache", ""))
  }

  test("validate describe formatted command to display column_meta_cache when column_meta_cache is set - COLUMN_META_CACHE_10") {
    sql("drop table if exists column_meta_cache")
    sql("create table column_meta_cache(c1 String, c2 String, c3 int, c4 double) STORED AS carbondata TBLPROPERTIES('COLUMN_meta_CachE'='c2')")
    val descResult = sql("describe formatted column_meta_cache")
    checkExistence(descResult, true, "Cached Min/Max Index Columns c2")
  }

  test("validate describe formatted command to display column_meta_cache when column_meta_cache is not set - COLUMN_META_CACHE_11") {
    sql("drop table if exists column_meta_cache")
    sql("create table column_meta_cache(c1 String, c2 String, c3 int, c4 double) STORED AS carbondata")
    val descResult = sql("describe formatted column_meta_cache")
    checkExistence(descResult, false, "Cached Min/Max Index Columns c2")
  }

  test("validate column_meta_cache after column drop - COLUMN_META_CACHE_12") {
    sql("drop table if exists column_meta_cache")
    sql("create table column_meta_cache(c1 String, c2 String, c3 int, c4 double) STORED AS carbondata TBLPROPERTIES('column_meta_cache'='c1,c2,c3')")
    assert(isExpectedValueValid("default", "column_meta_cache", "column_meta_cache", "c1,c2,c3"))
    sql("alter table column_meta_cache drop columns(c2)")
    assert(isExpectedValueValid("default", "column_meta_cache", "column_meta_cache", "c1,c3"))
  }

  test("validate cache_level with only empty spaces - CACHE_LEVEL_01") {
    sql("drop table if exists cache_level")
    intercept[MalformedCarbonCommandException] {
      sql("create table cache_level(c1 String) STORED AS carbondata TBLPROPERTIES('cache_level'='    ')")
    }
  }

  test("validate cache_level with invalid values - CACHE_LEVEL_02") {
    sql("drop table if exists cache_level")
    intercept[MalformedCarbonCommandException] {
      sql("create table cache_level(c1 String) STORED AS carbondata TBLPROPERTIES('cache_level'='xyz,abc')")
    }
  }

  test("validate cache_level with property in different cases - CACHE_LEVEL_03") {
    sql("drop table if exists cache_level")
    sql("create table cache_level(c1 String) STORED AS carbondata TBLPROPERTIES('CACHE_leveL'='BLOcK')")
    assert(isExpectedValueValid("default", "cache_level", "cache_level", "BLOCK"))
  }

  test("validate cache_level with default value as Blocklet - CACHE_LEVEL_04") {
    sql("drop table if exists cache_level")
    sql("create table cache_level(c1 String) STORED AS carbondata TBLPROPERTIES('cache_level'='bloCKlet')")
    assert(isExpectedValueValid("default", "cache_level", "cache_level", "BLOCKLET"))
  }

  test("validate describe formatted command to display cache_level when cache_level is set - CACHE_LEVEL_05") {
    sql("drop table if exists cache_level")
    sql("create table cache_level(c1 String) STORED AS carbondata TBLPROPERTIES('cache_level'='bloCKlet')")
    val descResult = sql("describe formatted cache_level")
    checkExistence(descResult, true, "Min/Max Index Cache Level BLOCKLET")
  }

  test("validate describe formatted command to display cache_level when cache_level is not set - CACHE_LEVEL_06") {
    sql("drop table if exists cache_level")
    sql("create table cache_level(c1 String) STORED AS carbondata")
    val descResult = sql("describe formatted cache_level")
    // even though not configured default cache level will be displayed as BLOCK
    checkExistence(descResult, true, "Min/Max Index Cache Level BLOCK")
  }

}

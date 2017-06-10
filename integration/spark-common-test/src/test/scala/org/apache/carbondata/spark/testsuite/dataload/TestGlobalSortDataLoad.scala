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

import java.io.{File, FilenameFilter}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class TestGlobalSortDataLoad extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  var filePath: String = s"$resourcesPath/globalsort"

  override def beforeEach {
    sql("DROP TABLE IF EXISTS carbon_globalSort")
    sql(
      """
        | CREATE TABLE carbon_globalSort(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
  }

  override def afterEach {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS,
        CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS_DEFAULT)

    sql(s"SET ${CarbonCommonConstants.LOAD_SORT_SCOPE} = ${CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT}")
    sql(s"SET ${CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS} = " +
      s"${CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS_DEFAULT}")

    sql("DROP TABLE IF EXISTS carbon_globalSort")
  }

  override def beforeAll {
    sql("DROP TABLE IF EXISTS carbon_localSort_once")
    sql(
      """
        | CREATE TABLE carbon_localSort_once(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localSort_once")
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS carbon_localSort_once")
    sql("DROP TABLE IF EXISTS carbon_localSort_twice")
    sql("DROP TABLE IF EXISTS carbon_localSort_triple")
    sql("DROP TABLE IF EXISTS carbon_localSort_delete")
    sql("DROP TABLE IF EXISTS carbon_localSort_update")
    sql("DROP TABLE IF EXISTS carbon_globalSort")
  }

  // ----------------------------------- Compare Result -----------------------------------
  test("Make sure the result is right and sorted in global level") {
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
      "OPTIONS('SORT_SCOPE'='GLOBAL_SORT', 'GLOBAL_SORT_PARTITIONS'='1')")

    assert(getIndexFileCount("carbon_globalSort") === 1)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalSort"), Seq(Row(12)))
    checkAnswer(sql("SELECT * FROM carbon_globalSort"),
      sql("SELECT * FROM carbon_localSort_once ORDER BY name"))
  }

  // ----------------------------------- Bad Record -----------------------------------
  test("Test GLOBAL_SORT with BAD_RECORDS_ACTION = 'FAIL'") {
    intercept[Exception] {
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
        "OPTIONS('SORT_SCOPE'='GLOBAL_SORT', 'BAD_RECORDS_ACTION'='FAIL')")
    }
  }

  test("Test GLOBAL_SORT with BAD_RECORDS_ACTION = 'REDIRECT'") {
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
      "OPTIONS('SORT_SCOPE'='GLOBAL_SORT', 'BAD_RECORDS_ACTION'='REDIRECT')")

    assert(getIndexFileCount("carbon_globalSort") === 3)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalSort"), Seq(Row(11)))
  }

  // ----------------------------------- Single Pass -----------------------------------
  test("Test GLOBAL_SORT with SINGLE_PASS") {
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
      "OPTIONS('SORT_SCOPE'='GLOBAL_SORT', 'SINGLE_PASS'='TRUE')")

    assert(getIndexFileCount("carbon_globalSort") === 3)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalSort"), Seq(Row(12)))
    checkAnswer(sql("SELECT * FROM carbon_globalSort ORDER BY name"),
      sql("SELECT * FROM carbon_localSort_once ORDER BY name"))
  }

  // ----------------------------------- Configuration Validity -----------------------------------
  test("Don't support GLOBAL_SORT on partitioned table") {
    sql("DROP TABLE IF EXISTS carbon_globalSort_partitioned")
    sql(
      """
        | CREATE TABLE carbon_globalSort_partitioned(name STRING, city STRING, age INT)
        | PARTITIONED BY (id INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('PARTITION_TYPE'='HASH','NUM_PARTITIONS'='3')
      """.stripMargin)

    intercept[MalformedCarbonCommandException] {
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort_partitioned " +
        "OPTIONS('SORT_SCOPE'='GLOBAL_SORT')")
    }
  }

  test("Number of partitions should be greater than 0") {
    intercept[MalformedCarbonCommandException] {
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
        "OPTIONS('SORT_SCOPE'='GLOBAL_SORT', 'GLOBAL_SORT_PARTITIONS'='0')")
    }

    intercept[MalformedCarbonCommandException] {
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
        "OPTIONS('SORT_SCOPE'='GLOBAL_SORT', 'GLOBAL_SORT_PARTITIONS'='a')")
    }
  }

  // ----------------------------------- Compaction -----------------------------------
  test("Compaction GLOBAL_SORT * 2") {
    sql("DROP TABLE IF EXISTS carbon_localSort_twice")
    sql(
      """
        | CREATE TABLE carbon_localSort_twice(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localSort_twice")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localSort_twice")

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
      s"OPTIONS('SORT_SCOPE'='GLOBAL_SORT')")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
      s"OPTIONS('SORT_SCOPE'='GLOBAL_SORT')")
    sql("ALTER TABLE carbon_globalSort COMPACT 'MAJOR'")

    assert(getIndexFileCount("carbon_globalSort") === 3)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalSort"), Seq(Row(24)))
    checkAnswer(sql("SELECT * FROM carbon_globalSort ORDER BY name"),
      sql("SELECT * FROM carbon_localSort_twice ORDER BY name"))
  }

  test("Compaction GLOBAL_SORT + LOCAL_SORT + BATCH_SORT") {
    sql("DROP TABLE IF EXISTS carbon_localSort_triple")
    sql(
      """
        | CREATE TABLE carbon_localSort_triple(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localSort_triple")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localSort_triple")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localSort_triple")

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
      s"OPTIONS('SORT_SCOPE'='GLOBAL_SORT')")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
      s"OPTIONS('SORT_SCOPE'='LOCAL_SORT')")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
      s"OPTIONS('SORT_SCOPE'='BATCH_SORT', 'BATCH_SORT_SIZE_INMB'='1')")
    sql("ALTER TABLE carbon_globalSort COMPACT 'MAJOR'")

    assert(getIndexFileCount("carbon_globalSort") === 3)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalSort"), Seq(Row(36)))
    checkAnswer(sql("SELECT * FROM carbon_globalSort ORDER BY name"),
      sql("SELECT * FROM carbon_localSort_triple ORDER BY name"))
  }

  // ----------------------------------- Check Configurations -----------------------------------
  // Waiting for merge SET feature[CARBONDATA-1065]
  ignore("DDL > SET") {
    sql(s"SET ${CarbonCommonConstants.LOAD_SORT_SCOPE} = LOCAL_SORT")
    sql(s"SET ${CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS} = 5")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
      "OPTIONS('SORT_SCOPE'='GLOBAL_SORT', 'GLOBAL_SORT_PARTITIONS'='2')")

    assert(getIndexFileCount("carbon_globalSort") === 2)
  }

  test("DDL > carbon.properties") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "LOCAL_SORT")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS, "5")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
      "OPTIONS('SORT_SCOPE'='GLOBAL_SORT', 'GLOBAL_SORT_PARTITIONS'='2')")

    assert(getIndexFileCount("carbon_globalSort") === 2)
  }

  // Waiting for merge SET feature[CARBONDATA-1065]
  ignore("SET > carbon.properties") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "LOCAL_SORT")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS, "5")
    sql(s"SET ${CarbonCommonConstants.LOAD_SORT_SCOPE} = GLOBAL_SORT")
    sql(s"SET ${CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS} = 2")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort")

    assert(getIndexFileCount("carbon_globalSort") === 2)
  }

  test("carbon.properties") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "GLOBAL_SORT")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS, "2")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort")

    assert(getIndexFileCount("carbon_globalSort") === 2)
  }

  // ----------------------------------- IUD -----------------------------------
  test("LOAD with DELETE") {
    sql("DROP TABLE IF EXISTS carbon_localSort_delete")
    sql(
      """
        | CREATE TABLE carbon_localSort_delete(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localSort_delete")
    sql("DELETE FROM carbon_localSort_delete WHERE id = 1")

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
      "OPTIONS('SORT_SCOPE'='GLOBAL_SORT')")
    sql("DELETE FROM carbon_globalSort WHERE id = 1")

    assert(getIndexFileCount("carbon_globalSort") === 3)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalSort"), Seq(Row(11)))
    checkAnswer(sql("SELECT * FROM carbon_globalSort ORDER BY name"),
      sql("SELECT * FROM carbon_localSort_delete ORDER BY name"))
  }

  // Some bugs in UPDATE feature, need to check.
  ignore("LOAD with UPDATE") {
    sql("DROP TABLE IF EXISTS carbon_localSort_update")
    sql(
      """
        | CREATE TABLE carbon_localSort_update(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localSort_update")
    sql("UPDATE carbon_localSort_update SET (name) = ('bb') WHERE id = 2")
    sql("SELECT * FROM carbon_localSort_update").show

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalSort " +
      "OPTIONS('SORT_SCOPE'='GLOBAL_SORT')")
    sql("UPDATE carbon_globalSort SET (name) = ('bb') WHERE id = 2")

    assert(getIndexFileCount("carbon_globalSort") === 3)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalSort"), Seq(Row(12)))
    checkAnswer(sql("SELECT name FROM carbon_localSort_update WHERE id = 2"), Seq(Row("bb")))
    checkAnswer(sql("SELECT name FROM carbon_globalSort WHERE id = 2"), Seq(Row("bb")))
    checkAnswer(sql("SELECT * FROM carbon_globalSort ORDER BY name"),
      sql("SELECT * FROM carbon_localSort_update ORDER BY name"))
  }

  // ----------------------------------- INSERT INTO -----------------------------------
  test("INSERT INTO") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "GLOBAL_SORT")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS, "2")
    sql(s"INSERT INTO TABLE carbon_globalSort SELECT * FROM carbon_localSort_once")

    assert(getIndexFileCount("carbon_globalSort") === 2)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalSort"), Seq(Row(12)))
    checkAnswer(sql("SELECT * FROM carbon_globalSort ORDER BY name"),
      sql("SELECT * FROM carbon_localSort_once ORDER BY name"))
  }

  private def getIndexFileCount(tableName: String, segmentNo: String = "0"): Int = {
    val store  = storeLocation +"/default/"+ tableName + "/Fact/Part0/Segment_"+segmentNo
    val list = new File(store).list(new FilenameFilter {
      override def accept(dir: File, name: String) = name.endsWith(".carbonindex")
    })
    list.size
  }
}

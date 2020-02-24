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

package org.apache.carbondata.integration.spark.testsuite.dataload

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Ignore}

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.util.CarbonProperties

/**
 * test data load with unsafe memory.
 * The CI env may not have so much memory, so disable this test case for now.
 * Ps: seen from CI result, the sdvTests works fine
 */
@Ignore
class TestLoadDataWithUnsafeMemory extends QueryTest
  with BeforeAndAfterEach with BeforeAndAfterAll {
  val originUnsafeSortStatus: String = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT,
      CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
  val originUnsafeMemForSort: String = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB,
      CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB_DEFAULT)
  val originUnsafeMemForWorking: String = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB,
      CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB_DEFAULT)
  val originUnsafeSizeForChunk: String = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.OFFHEAP_SORT_CHUNK_SIZE_IN_MB,
      CarbonCommonConstants.OFFHEAP_SORT_CHUNK_SIZE_IN_MB_DEFAULT)
  val originSpillPercentage: String = CarbonProperties.getInstance()
    .getProperty(CarbonLoadOptionConstants.CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE,
      CarbonLoadOptionConstants.CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE_DEFAULT)
  val targetTable = "table_unsafe_memory"


  override def beforeEach(): Unit = {
    sql(s"drop table if exists $targetTable ")
  }

  override def afterEach(): Unit = {
    sql(s"drop table if exists $targetTable ")
  }

  override protected def beforeAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB, "1024")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, "512")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.OFFHEAP_SORT_CHUNK_SIZE_IN_MB, "512")
    CarbonProperties.getInstance()
      .addProperty(CarbonLoadOptionConstants.CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE, "-1")
  }

  override def afterAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, originUnsafeSortStatus)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB, originUnsafeMemForSort)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, originUnsafeMemForWorking)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.OFFHEAP_SORT_CHUNK_SIZE_IN_MB, originUnsafeSizeForChunk)
    CarbonProperties.getInstance()
      .addProperty(CarbonLoadOptionConstants.CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE,
        originSpillPercentage)
  }

  private def testSimpleTable(): Unit = {
    // This number is chosen to reproduce issue CARBONDATA-2246. It was choose on purpose that the
    // records in memory will consume about two more unsafe-row-pages and the last one will exhaust
    // the working memory.
    val lineNum: Int = 70002
    val df = {
      import sqlContext.implicits._
      sqlContext.sparkContext.parallelize((1 to lineNum).reverse)
        .map(x => (s"a$x", s"b$x", s"c$x", 12.3 + x, x, System.currentTimeMillis(), s"d$x"))
        .toDF("c1", "c2", "c3", "c4", "c5", "c6", "c7")
    }

    df.write
      .format("carbondata")
      .option("tableName", targetTable)
      .option("SORT_COLUMNS", "c1,c3")
      .save()

    checkAnswer(sql(s"select count(*) from $targetTable"), Row(lineNum))
    checkAnswer(sql(s"select count(*) from $targetTable where c5 > 5000"), Row(lineNum - 5000))
  }

  // see CARBONDATA-2246
  test("unsafe sort with chunk size equal to working memory") {
    testSimpleTable()
  }
}

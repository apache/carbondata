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

package org.apache.carbondata.spark.testsuite.detailquery

import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.spark.util.DataGenerator
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{CarbonSession, Row, SaveMode}

/**
 * Test Suite for set feature of search mode
 */

class SearchModeSetTestCase extends QueryTest with BeforeAndAfterAll {

  val numRows = 500 * 1000

  override def beforeAll = {
    sqlContext.sparkContext.setLogLevel("INFO")
    sql("DROP TABLE IF EXISTS main")

    val df = DataGenerator.generateDataFrame(sqlContext.sparkSession, numRows)
    df.write
      .format("carbondata")
      .option("tableName", "main")
      .option("table_blocksize", "5")
      .mode(SaveMode.Overwrite)
      .save()
  }

  override def afterAll = {
    sql("DROP TABLE IF EXISTS main")
    sqlContext.sparkSession.asInstanceOf[CarbonSession].stopSearchMode()
  }

  private def sparkSql(sql: String): Seq[Row] = {
    sqlContext.sparkSession.asInstanceOf[CarbonSession].sparkSql(sql).collect()
  }

  private def checkSearchAnswer(query: String) = {
    checkAnswer(sql(query), sparkSql(query))
  }

  test("set search mode") {
    sqlContext.sparkSession.asInstanceOf[CarbonSession].stopSearchMode()
    sql("set carbon.search.enabled = true")
    assert(sqlContext.sparkSession.asInstanceOf[CarbonSession].isSearchModeEnabled)
    checkSearchAnswer("select id from main where id = '3'")
    checkSearchAnswer("select id from main where id = '3' limit 10")
    sql("set carbon.search.enabled = false")
    assert(!sqlContext.sparkSession.asInstanceOf[CarbonSession].isSearchModeEnabled)
  }
}

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

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{CarbonSession, Row, SaveMode}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.util.DataGenerator

/**
 * Test Suite for search mode
 */

class SearchModeTestCase extends QueryTest with BeforeAndAfterAll {

  val numRows = 500 * 1000
  override def beforeAll = {
    sqlContext.sparkSession.asInstanceOf[CarbonSession].startSearchMode()
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
    sql("set carbon.search.enabled = false")
    sqlContext.sparkSession.asInstanceOf[CarbonSession].stopSearchMode()
  }

  private def sparkSql(sql: String): Seq[Row] = {
    sqlContext.sparkSession.asInstanceOf[CarbonSession].sparkSql(sql).collect()
  }

  private def checkSearchAnswer(query: String) = {
    checkAnswer(sql(query), sparkSql(query))
  }

  test("SearchMode Query: row result") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "false")
    checkSearchAnswer("select * from main where city = 'city3'")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
      CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
  }

  test("SearchMode Query: vector result") {
    checkSearchAnswer("select * from main where city = 'city3'")
  }

  test("equal filter") {
    checkSearchAnswer("select id from main where id = '100'")
    checkSearchAnswer("select id from main where planet = 'planet100'")
  }

  test("greater and less than filter") {
    checkSearchAnswer("select id from main where m2 < 4")
  }

  test("IN filter") {
    checkSearchAnswer("select id from main where id IN ('40', '50', '60')")
  }

  test("expression filter") {
    checkSearchAnswer("select id from main where length(id) < 2")
  }

  test("filter with limit") {
    checkSearchAnswer("select id from main where id = '3' limit 10")
    checkSearchAnswer("select id from main where length(id) < 2 limit 10")
  }

  test("aggregate query") {
    checkSearchAnswer("select city, sum(m1) from main where m2 < 10 group by city")
  }

  test("aggregate query with datamap and fallback to SparkSQL") {
    sql("create datamap preagg on table main using 'preaggregate' as select city, count(*) from main group by city ")
    checkSearchAnswer("select city, count(*) from main group by city")
    sql("drop datamap preagg on table main").show()
  }

  test("set search mode") {
    sql("set carbon.search.enabled = true")
    assert(sqlContext.sparkSession.asInstanceOf[CarbonSession].isSearchModeEnabled)
    checkSearchAnswer("select id from main where id = '3' limit 10")
    sql("set carbon.search.enabled = false")
    assert(!sqlContext.sparkSession.asInstanceOf[CarbonSession].isSearchModeEnabled)
  }

  test("test lucene datamap with search mode") {
    sql("set carbon.search.enabled = true")
    sql("DROP DATAMAP IF EXISTS dm ON TABLE main")
    sql("CREATE DATAMAP dm ON TABLE main USING 'lucene' DMProperties('INDEX_COLUMNS'='id') ")
    checkAnswer(sql("SELECT * FROM main WHERE TEXT_MATCH('id:100000')"),
      sql(s"SELECT * FROM main WHERE id='100000'"))
    sql("DROP DATAMAP if exists dm ON TABLE main")
    sql("set carbon.search.enabled = false")
  }

  test("test lucene datamap with search mode 2") {
    sql("drop datamap if exists dm3 ON TABLE main")
    sql("CREATE DATAMAP dm3 ON TABLE main USING 'lucene' DMProperties('INDEX_COLUMNS'='city') ")
    checkAnswer(sql("SELECT * FROM main WHERE TEXT_MATCH('city:city6')"),
      sql("SELECT * FROM main WHERE city='city6'"))
    sql("DROP DATAMAP if exists dm3 ON TABLE main")
  }

  test("test lucene datamap with search mode, two column") {
    sql("drop datamap if exists dm3 ON TABLE main")
    sql("CREATE DATAMAP dm3 ON TABLE main USING 'lucene' DMProperties('INDEX_COLUMNS'='city , id') ")
    checkAnswer(sql("SELECT * FROM main WHERE TEXT_MATCH('city:city6')"),
      sql("SELECT * FROM main WHERE city='city6'"))
    checkAnswer(sql("SELECT * FROM main WHERE TEXT_MATCH('id:100000')"),
      sql(s"SELECT * FROM main WHERE id='100000'"))
    sql("DROP DATAMAP if exists dm3 ON TABLE main")
  }

  test("start search mode twice") {
    sqlContext.sparkSession.asInstanceOf[CarbonSession].startSearchMode()
    assert(sqlContext.sparkSession.asInstanceOf[CarbonSession].isSearchModeEnabled)
    checkSearchAnswer("select id from main where id = '3' limit 10")
    sqlContext.sparkSession.asInstanceOf[CarbonSession].stopSearchMode()
    assert(!sqlContext.sparkSession.asInstanceOf[CarbonSession].isSearchModeEnabled)

    // start twice
    sqlContext.sparkSession.asInstanceOf[CarbonSession].startSearchMode()
    assert(sqlContext.sparkSession.asInstanceOf[CarbonSession].isSearchModeEnabled)
    checkSearchAnswer("select id from main where id = '3' limit 10")
    sqlContext.sparkSession.asInstanceOf[CarbonSession].stopSearchMode()
  }
}

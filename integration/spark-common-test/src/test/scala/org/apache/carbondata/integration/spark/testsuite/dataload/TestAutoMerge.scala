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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

/**
  * Test class of creating and loading for carbon table with auto load merge
  */
class TestAutoMerge extends QueryTest with BeforeAndAfterEach {

  override def beforeEach: Unit = {
    sql("DROP TABLE IF EXISTS automerge")
    sql("DROP TABLE IF EXISTS automerge2")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
    sql(
      """
         CREATE TABLE automerge(id int, name string, city string, age int)
         STORED BY 'org.apache.carbondata.format'
      """)
    sql(
      """
         CREATE TABLE automerge2(id int, name string, city string, age int)
         STORED BY 'org.apache.carbondata.format'
      """)
  }

  test("test data loading with auto load merge, load once") {
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table automerge")
    checkAnswer(
      sql("SELECT COUNT(*) FROM automerge"),
      Seq(Row(4))
    )
  }

  test("test data loading with auto load merge, load four times") {
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table automerge")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table automerge")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table automerge")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table automerge")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE automerge"), true, "Compacted")
    val segments = sql("SHOW SEGMENTS FOR TABLE automerge")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    checkAnswer(
      sql("SELECT COUNT(*) FROM automerge"),
      Seq(Row(16))
    )
  }

  test("Inserting into: auto load merge, load four times") {
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table automerge")
    for (i <- 0 until 4) {
      sql("insert into automerge2 select * from automerge")
    }
    checkExistence(sql("SHOW SEGMENTS FOR TABLE automerge2"), true, "Compacted")
    val segments = sql("SHOW SEGMENTS FOR TABLE automerge2")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    checkAnswer(
      sql("SELECT COUNT(*) FROM automerge2"),
      Seq(Row(16))
    )
  }

  test("Inserting overwrite: auto load merge, load four times") {
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table automerge")
    for (i <- 0 until 4) {
      sql("insert overwrite table automerge2 select * from automerge")
    }
    checkExistence(sql("SHOW SEGMENTS FOR TABLE automerge2"), false, "Compacted")
    val segments = sql("SHOW SEGMENTS FOR TABLE automerge2")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(!SegmentSequenceIds.contains("0.1"))
    checkAnswer(
      sql("SELECT COUNT(*) FROM automerge2"),
      Seq(Row(4))
    )
  }

  override def afterEach: Unit = {
    sql("DROP TABLE IF EXISTS automerge")
    sql("DROP TABLE IF EXISTS automerge2")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  }
}

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

package org.apache.carbondata.spark.testsuite.datacompaction

import java.io.{File, PrintWriter}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class TableLevelCompactionOptionTest extends QueryTest
  with BeforeAndAfterEach with BeforeAndAfterAll {

  val tempFilePath: String = s"$resourcesPath/temp/tableLevelCompactionParaTest.csv"
  val sampleFilePath: String = resourcesPath + "/sample.csv"

  override def beforeEach {
    cleanTable()
  }

  override def afterEach {
    resetConf()
    cleanTable()
  }

  private def resetConf() ={
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_MAJOR_COMPACTION_SIZE,
      CarbonCommonConstants.DEFAULT_CARBON_MAJOR_COMPACTION_SIZE)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
        CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER,
        CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT,
        CarbonCommonConstants.DEFAULT_DAYS_ALLOWED_TO_COMPACT)
  }

  private def cleanTable() = {
    deleteTempFile()
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  private def generateTempFile() = {
    val writer = new PrintWriter(new File(tempFilePath))
    try {
      writer.println("id,name,city,age")
      val lines =
        s"""|1,david,shenzhen,31
            |2,eason,shenzhen,27
            |3,jarry,wuhan,35
            |3,jarry,Bangalore,35
            |4,kunal,Delhi,26
            |4,vishal,Bangalore,29""".stripMargin
      for (i <- 0 until 250000) {
        writer.println(lines)
      }
      writer.flush()
    } finally {
      if (writer != null) writer.close()
    }
  }

  private def deleteTempFile() = {
    val file = new File(tempFilePath)
    if (file.exists()) {
      file.delete()
    }
  }

  test("MAJOR_COMPACTION_SIZE, use system level configuration"){
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MAJOR_COMPACTION_SIZE, "10")

    // generate a temp file which is larger than 1M but smaller than 5M
    generateTempFile()

    sql(
      """
        |CREATE TABLE carbon_table
        |(id INT, name STRING, city STRING, age INT)
        |STORED AS carbondata
        |TBLPROPERTIES('SORT_COLUMNS'='city,name')
      """.stripMargin)

    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$tempFilePath' INTO TABLE carbon_table")
    }

    sql("ALTER TABLE carbon_table COMPACT 'MAJOR'")
    sql("CLEAN FILES FOR TABLE carbon_table")

    val segments = sql("SHOW SEGMENTS FOR TABLE carbon_table")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(!SegmentSequenceIds.contains("0"))
    assert(!SegmentSequenceIds.contains("1"))
    assert(SegmentSequenceIds.contains("0.1"))

  }

  test("MAJOR_COMPACTION_SIZE, use table level configuration") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MAJOR_COMPACTION_SIZE, "10")

    // generate a temp file which is larger than 1M but smaller than 5M
    generateTempFile()

    sql(
      """
        |CREATE TABLE carbon_table
        |(id INT, name STRING, city STRING, age INT)
        |STORED AS carbondata
        |TBLPROPERTIES('SORT_COLUMNS'='city,name',
        |'MAJOR_COMPACTION_SIZE'='1', 'LOCAL_DICTIONARY_ENABLE'='false')
      """.stripMargin)

    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$tempFilePath' INTO TABLE carbon_table")
    }

    // each segment is larger than 1M, so no segments will be compacted
    sql("ALTER TABLE carbon_table COMPACT 'MAJOR'")
    sql("CLEAN FILES FOR TABLE carbon_table")

    val segments = sql("SHOW SEGMENTS FOR TABLE carbon_table")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0"))
    assert(SegmentSequenceIds.contains("1"))
    assert(!SegmentSequenceIds.contains("0.1"))

    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_MAJOR_COMPACTION_SIZE,
      CarbonCommonConstants.DEFAULT_CARBON_MAJOR_COMPACTION_SIZE)
  }

  test("ENABLE_AUTO_LOAD_MERGE: true, use system level configuration"){
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "4,2")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER, "0")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT, "0")

    sql(
      """
        | CREATE TABLE carbon_table(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name')
      """.stripMargin)

    for (i <- 0 until 8) {
      sql(s"LOAD DATA LOCAL INPATH '$sampleFilePath' INTO TABLE carbon_table")
    }
    sql("CLEAN FILES FOR TABLE carbon_table")
    var segments = sql("SHOW SEGMENTS FOR TABLE carbon_table")
    var segmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(segmentSequenceIds.size==1)
    assert(segmentSequenceIds.contains("0.2"))
  }

  test("ENABLE_AUTO_LOAD_MERGE: false, use table level configuration"){
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "4,2")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER, "0")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT, "0")

    sql(
      """
        | CREATE TABLE carbon_table(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name',
        | 'AUTO_LOAD_MERGE'='false')
      """.stripMargin)

    for (i <- 0 until 8) {
      sql(s"LOAD DATA LOCAL INPATH '$sampleFilePath' INTO TABLE carbon_table")
    }
    // table level configuration: 'AUTO_LOAD_MERGE'='false', so no segments will be compacted
    checkExistence(sql("SHOW SEGMENTS FOR TABLE carbon_table"), false, "Compacted")
    var segments = sql("SHOW SEGMENTS FOR TABLE carbon_table")
    var segmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(segmentSequenceIds.size==8)
    assert(!segmentSequenceIds.contains("0.1"))
    assert(!segmentSequenceIds.contains("4.1"))
    assert(!segmentSequenceIds.contains("0.2"))
  }

  test("ENABLE_AUTO_LOAD_MERGE: true, use table level configuration") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "4,2")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER, "0")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT, "0")

    sql(
      """
         | CREATE TABLE carbon_table(id INT, name STRING, city STRING, age INT)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='city,name',
         | 'AUTO_LOAD_MERGE'='true',
         | 'COMPACTION_LEVEL_THRESHOLD'='3,2',
         | 'COMPACTION_PRESERVE_SEGMENTS'='2',
         | 'TABLE_ALLOWED_COMPACTION_DAYS'='1')
      """.stripMargin)

    // load 6 segments, the latest 2 segments are preserved
    // only one level-1 minor compaction is triggered which compacts segment 0,1,2 to segment 0.1
    // seg0 \
    // seg1  -- compacted to seg0.1
    // seg2 /
    // seg3
    // seg4 (preserved)
    // seg5 (preserved)
    for (i <- 0 until 6) {
      sql(s"LOAD DATA LOCAL INPATH '$sampleFilePath' INTO TABLE carbon_table")
    }
    sql("CLEAN FILES FOR TABLE carbon_table")
    var segments = sql("SHOW SEGMENTS FOR TABLE carbon_table")
    var segmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(segmentSequenceIds.contains("0.1"))
    assert(!segmentSequenceIds.contains("3.1"))
    assert(!segmentSequenceIds.contains("0.2"))

    // load another two segments, the latest 2 segments are preserved
    // level-2 minor compaction is triggered which compacts segment 0,1,2,3,4,5 -> 0.2
    // seg0 \
    // seg1  -- compacted to seg0.1 \
    // seg2 /                         -- compacted to seg0.2
    // seg3 \                       /
    // seg4  -- compacted to seg3.1
    // seg5 /
    // seg6 (preserved)
    // seg7 (preserved)
    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$sampleFilePath' INTO TABLE carbon_table")
    }
    sql("CLEAN FILES FOR TABLE carbon_table")
    segments = sql("SHOW SEGMENTS FOR TABLE carbon_table")
    segmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(segmentSequenceIds.contains("0.2"))
    assert(!segmentSequenceIds.contains("0.1"))
    assert(!segmentSequenceIds.contains("3.1"))
  }

  test("AUTO MERGE TRUE:Verify 2nd Level compaction equals to 1"){
    sql("DROP TABLE IF EXISTS tablecompaction_table")
    sql(
      """
        |create table tablecompaction_table(
        |name string,age int) STORED AS carbondata
        |tblproperties('AUTO_LOAD_MERGE'='true','COMPACTION_LEVEL_THRESHOLD'='2,1')
      """.stripMargin)

    for(i <-0 until 4){
      sql("insert into tablecompaction_table select 'a',12")
    }
    var segments = sql("SHOW SEGMENTS FOR TABLE tablecompaction_table")
    var segmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(segmentSequenceIds.size==6)
    assert(segmentSequenceIds.contains("0.1"))
    assert(segmentSequenceIds.contains("2.1"))
  }

  test("AUTO MERGE FALSE:Verify 2nd Level compaction equals to 1"){
    sql("DROP TABLE IF EXISTS tablecompaction_table")
    sql(
      """
        |create table tablecompaction_table(
        |name string,age int) STORED AS carbondata
        |tblproperties('COMPACTION_LEVEL_THRESHOLD'='2,1')
      """.stripMargin)

    for(i <-0 until 4){
      sql("insert into tablecompaction_table select 'a',12")
    }
    sql("alter table tablecompaction_table compact 'minor' ")
    var segments = sql("SHOW SEGMENTS FOR TABLE tablecompaction_table")
    var segmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(segmentSequenceIds.size==6)
    assert(segmentSequenceIds.contains("0.1"))
    assert(segmentSequenceIds.contains("2.1"))
  }

  // 2nd Level compaction value = 0 is supported by system level(like 6,0)
  // same need to support for table level also
  test("Verify 2nd Level compaction equals to 0"){
    sql("DROP TABLE IF EXISTS tablecompaction_table")
    sql(
      """
        |create table tablecompaction_table(
        |name string,age int) STORED AS carbondata
        |tblproperties('AUTO_LOAD_MERGE'='true','COMPACTION_LEVEL_THRESHOLD'='2,0')
      """.stripMargin)

    for(i <-0 until 4){
      sql("insert into tablecompaction_table select 'a',12")
    }
    var segments = sql("SHOW SEGMENTS FOR TABLE tablecompaction_table")
    var segmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(segmentSequenceIds.size==6)
    assert(segmentSequenceIds.contains("0.1"))
    assert(segmentSequenceIds.contains("2.1"))
  }

  test("System Level:Verify 2nd Level compaction equals to 1"){
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,1")
    sql("DROP TABLE IF EXISTS tablecompaction_table")
    sql(
      """
        |create table tablecompaction_table(
        |name string,age int) STORED AS carbondata
      """.stripMargin)

    for(i <-0 until 4){
      sql("insert into tablecompaction_table select 'a',12")
    }
    sql("alter table tablecompaction_table compact 'minor' ")
    var segments = sql("SHOW SEGMENTS FOR TABLE tablecompaction_table")
    var segmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(segmentSequenceIds.size==6)
    assert(segmentSequenceIds.contains("0.1"))
    assert(segmentSequenceIds.contains("2.1"))
  }

}

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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore
import org.apache.carbondata.core.metadata.{CarbonMetadata, SegmentFileStore}
import org.apache.carbondata.core.util.path.CarbonTablePath

class CompactionSupportGlobalSortParameterTest extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val filePath: String = s"$resourcesPath/globalsort"
  val file1: String = resourcesPath + "/globalsort/sample1.csv"
  val file2: String = resourcesPath + "/globalsort/sample2.csv"
  val file3: String = resourcesPath + "/globalsort/sample3.csv"

  override def beforeEach {
    resetConf
    
    sql("DROP TABLE IF EXISTS compaction_globalsort")
    sql(
      """
        | CREATE TABLE compaction_globalsort(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT', 'GLOBAL_SORT_PARTITIONS'='1')
      """.stripMargin)

    sql("DROP TABLE IF EXISTS carbon_localsort")
    sql(
      """
        | CREATE TABLE carbon_localsort(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
  }

  override def afterEach {
    sql("DROP TABLE IF EXISTS compaction_globalsort")
    sql("DROP TABLE IF EXISTS carbon_localsort")
    resetConf()
  }

  test("MINOR, ENABLE_AUTO_LOAD_MERGE: false") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("delete from table compaction_globalsort where SEGMENT.ID in (1,2,3)")
    sql("delete from table carbon_localsort where SEGMENT.ID in (1,2,3)")
    sql("ALTER TABLE compaction_globalsort COMPACT 'minor'")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), false, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(!SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 6)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(12)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Success")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Marked for Delete")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
      CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  }

  test("MINOR, ENABLE_AUTO_LOAD_MERGE: true") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))

    // loaded 6 times and produced 6 segments,
    // auto merge will compact and produce 1 segment because 6 is bigger than 4 (default value of minor),
    // so total segment number is 7
    assert(SegmentSequenceIds.length == 7)

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
      CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  }

  test("MINOR, PRESERVE_LATEST_SEGMENTS_NUMBER: 0") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER,
      "0")
    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort")

    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("ALTER TABLE compaction_globalsort COMPACT 'MINOR'")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(!SegmentSequenceIds.contains("4.1"))
    assert(SegmentSequenceIds.length == 7)

    val status = segments.collect().map { each => (each.toSeq) (1) }
    assert(status.filter(_.equals("Compacted")).length == 4)

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 1)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(24)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER,
      CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER)
  }

  test("MINOR, PRESERVE_LATEST_SEGMENTS_NUMBER: 4") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER,
      "4")
    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort")

    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("ALTER TABLE compaction_globalsort COMPACT 'MINOR'")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), false, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(!SegmentSequenceIds.contains("0.1"))
    assert(!SegmentSequenceIds.contains("4.1"))
    assert(SegmentSequenceIds.length == 6)

    val status = segments.collect().map { each => (each.toSeq) (1) }
    assert(status.filter(_.equals("Compacted")).length == 0)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(24)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER,
      CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER)
  }

  test("MINOR, DAYS_ALLOWED_TO_COMPACT: 0") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT,
      "0")
    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort")

    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("ALTER TABLE compaction_globalsort COMPACT 'MINOR'")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(!SegmentSequenceIds.contains("4.1"))
    assert(SegmentSequenceIds.length == 7)

    val status = segments.collect().map { each => (each.toSeq) (1) }
    assert(status.filter(_.equals("Compacted")).length == 4)

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 1)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(24)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT,
      CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT)
  }

  test("MINOR, DAYS_ALLOWED_TO_COMPACT: 4") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT,
      "4")
    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort")

    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("ALTER TABLE compaction_globalsort COMPACT 'MINOR'")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(!SegmentSequenceIds.contains("4.1"))
    assert(SegmentSequenceIds.length == 7)

    val status = segments.collect().map { each => (each.toSeq) (1) }
    assert(status.filter(_.equals("Compacted")).length == 4)

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 1)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(24)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT,
      CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT)
  }

  test("MAJOR, ENABLE_AUTO_LOAD_MERGE: false") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("delete from table compaction_globalsort where SEGMENT.ID in (1,2,3)")
    sql("delete from table carbon_localsort where SEGMENT.ID in (1,2,3)")
    sql("ALTER TABLE compaction_globalsort COMPACT 'MAJOR'")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 7)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(12)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Success")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Marked for Delete")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
      CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  }

  test("MAJOR, ENABLE_AUTO_LOAD_MERGE: true") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")
    sql("ALTER TABLE compaction_globalsort COMPACT 'MAJOR'")
    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))

    // loaded 6 times and produced 6 segments,
    // auto merge will compact and produce 1 segment because 6 is bigger than 4 (default value of minor),
    // major compact and prodece 1 segment
    // so total segment number is 8
    assert(SegmentSequenceIds.length == 8)

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
      CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  }

  test("MAJOR, PRESERVE_LATEST_SEGMENTS_NUMBER: 0") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER,
      "0")
    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort")

    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("ALTER TABLE compaction_globalsort COMPACT 'MAJOR'")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(!SegmentSequenceIds.contains("4.1"))
    assert(SegmentSequenceIds.length == 7)

    val status = segments.collect().map { each => (each.toSeq) (1) }
    assert(status.filter(_.equals("Compacted")).length == 6)

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 1)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(24)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER,
      CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER)
  }

  test("MAJOR, PRESERVE_LATEST_SEGMENTS_NUMBER: 4") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER,
      "4")
    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort")

    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("ALTER TABLE compaction_globalsort COMPACT 'MAJOR'")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(!SegmentSequenceIds.contains("4.1"))
    assert(SegmentSequenceIds.length == 7)

    val status = segments.collect().map { each => (each.toSeq) (1) }
    assert(status.filter(_.equals("Compacted")).length == 2)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(24)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER,
      CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER)
  }

  test("MAJOR, DAYS_ALLOWED_TO_COMPACT: 0") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT,
      "0")
    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort")

    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("ALTER TABLE compaction_globalsort COMPACT 'MAJOR'")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(!SegmentSequenceIds.contains("4.1"))
    assert(SegmentSequenceIds.length == 7)

    val status = segments.collect().map { each => (each.toSeq) (1) }
    assert(status.filter(_.equals("Compacted")).length == 6)

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 1)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(24)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT,
      CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT)
  }

  test("MAJOR, DAYS_ALLOWED_TO_COMPACT: 4") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT,
      "4")
    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort")

    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("ALTER TABLE compaction_globalsort COMPACT 'MAJOR'")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(!SegmentSequenceIds.contains("4.1"))
    assert(SegmentSequenceIds.length == 7)

    val status = segments.collect().map { each => (each.toSeq) (1) }
    assert(status.filter(_.equals("Compacted")).length == 6)

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 1)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(24)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT,
      CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT)
  }

  test("MAJOR, ENABLE_PREFETCH_DURING_COMPACTION: true") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_ENABLE, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
    for (i <- 0 until 2) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("delete from table compaction_globalsort where SEGMENT.ID in (1,2,3)")
    sql("delete from table carbon_localsort where SEGMENT.ID in (1,2,3)")
    sql("ALTER TABLE compaction_globalsort COMPACT 'MAJOR'")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 7)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(12)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Success")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Marked for Delete")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
      CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_ENABLE,
      CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_ENABLE_DEFAULT)
  }

  private def resetConf() {
    val prop = CarbonProperties.getInstance()
    prop.addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
    prop.addProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS, CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS_DEFAULT)
    prop.addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

  private def getIndexFileCount(tableName: String, segmentNo: String = "0"): Int = {
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", tableName)
    val segmentDir = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, segmentNo)
    if (FileFactory.isFileExist(segmentDir)) {
      new SegmentIndexFileStore().getIndexFilesFromSegment(segmentDir).size()
    } else {
      val segment = Segment.getSegment(segmentNo, carbonTable.getTablePath)
      new SegmentFileStore(carbonTable.getTablePath, segment.getSegmentFileName).getIndexCarbonFiles.size()
    }
  }
}

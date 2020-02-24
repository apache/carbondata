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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore
import org.apache.carbondata.core.metadata.{CarbonMetadata, SegmentFileStore}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

class CompactionSupportGlobalSortFunctionTest extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
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
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='global_sort', 'GLOBAL_SORT_PARTITIONS'='1')
      """.stripMargin)

    sql("DROP TABLE IF EXISTS carbon_localsort")
    sql(
      """
        | CREATE TABLE carbon_localsort(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
      """.stripMargin)
  }

  override def afterEach {
    sql("DROP TABLE IF EXISTS compaction_globalsort")
    sql("DROP TABLE IF EXISTS carbon_localsort")
    resetConf
  }

  test("Compaction type: major") {
    sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
    sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

    sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort")

    sql("alter table compaction_globalsort set tblproperties('global_sort_partitions'='1')")
    sql("ALTER TABLE compaction_globalsort COMPACT 'MAJOR'")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 4)

    val status = segments.collect().map { each => (each.toSeq) (1) }
    assert(status.filter(_.equals("Compacted")).length == 3)

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 1)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(12)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    checkAnswer(sql("SELECT * FROM compaction_globalsort limit 3"),
      sql("SELECT * FROM carbon_localsort order by city,name limit 3"))
  }

  test("Compaction type: minor, < default segments in level 1, not compact") {
    sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
    sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

    sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("alter table compaction_globalsort set tblproperties('global_sort_partitions'='1')")
    sql("ALTER TABLE compaction_globalsort COMPACT 'MINOR'")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), false, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(!SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 3)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(12)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))
  }

  test("Compaction type: minor, >= default segments and < (default segments)*2 in level 1, compact once") {
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

    sql("alter table compaction_globalsort set tblproperties('global_sort_partitions'='1')")
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
  }

  test("Compaction type: minor, >= default segments in level 1,compact twice in level 1") {
    for (i <- 0 until 3) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort")
    }

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("alter table compaction_globalsort set tblproperties('global_sort_partitions'='1')")
    sql("ALTER TABLE compaction_globalsort COMPACT 'MINOR'")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.contains("4.1"))
    assert(!SegmentSequenceIds.contains("0.2"))
    assert(SegmentSequenceIds.length == 11)
    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 1)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(36)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))
  }

  test("Compaction type: minor, >= compacted segments in level 2,compact once in level 2") {
    for (i <- 0 until 4) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort")
    }

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("alter table compaction_globalsort set tblproperties('global_sort_partitions'='1')")
    sql("ALTER TABLE compaction_globalsort COMPACT 'MINOR'")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.contains("4.1"))
    assert(SegmentSequenceIds.contains("8.1"))
    assert(SegmentSequenceIds.contains("0.2"))
    assert(SegmentSequenceIds.length == 16)
    assert(getIndexFileCount("compaction_globalsort", "0.1") === 1)

    val status = segments.collect().map { each => (each.toSeq) (1) }
    assert(status.filter(_.equals("Compacted")).length == 15)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(48)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    checkAnswer(sql("SELECT * FROM compaction_globalsort limit 12"),
      sql("SELECT * FROM carbon_localsort order by city,name limit 12"))
  }

  test("Compaction: clean files, major") {
    for (i <- 0 until 1) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort")
    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("alter table compaction_globalsort set tblproperties('global_sort_partitions'='1')")
    sql("ALTER TABLE compaction_globalsort COMPACT 'major'")
    sql("clean files for table compaction_globalsort")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), false, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 1)

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 1)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(12)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    checkAnswer(sql("SELECT * FROM compaction_globalsort limit 3"),
      sql("SELECT * FROM carbon_localsort order by city,name limit 3"))
  }

  test("Compaction: clean files, minor") {
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
    sql("alter table compaction_globalsort set tblproperties('global_sort_partitions'='1')")
    sql("ALTER TABLE compaction_globalsort COMPACT 'minor'")
    sql("clean files for table compaction_globalsort")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), false, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 3)

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 1)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(24)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))
  }

  test("Compaction: global_sort_partitions=1, major") {
    for (i <- 0 until 1) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='1')")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='1')")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='1')")
    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("alter table compaction_globalsort set tblproperties('global_sort_partitions'='1')")
    sql("ALTER TABLE compaction_globalsort COMPACT 'major'")
    sql("clean files for table compaction_globalsort")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), false, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 1)

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 1)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(12)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    checkAnswer(sql("SELECT * FROM compaction_globalsort limit 3"),
      sql("SELECT * FROM carbon_localsort order by city,name limit 3"))
  }

  test("Compaction: global_sort_partitions=2, major") {
    for (i <- 0 until 1) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    sql("alter table compaction_globalsort set tblproperties('global_sort_partitions'='2')")
    sql("ALTER TABLE compaction_globalsort COMPACT 'major'")
    sql("clean files for table compaction_globalsort")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), false, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 1)

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 2)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(12)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))
  }

  test("Compaction: delete, major") {
    for (i <- 0 until 1) {
      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort")

      sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
      sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")
    }
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")
    sql("alter table compaction_globalsort set tblproperties('global_sort_partitions'='2')")
    sql("ALTER TABLE compaction_globalsort COMPACT 'major'")
    sql("clean files for table compaction_globalsort")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), false, "Compacted")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 1)

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 2)

    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(12)))

    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM carbon_localsort"))

    sql("delete from table compaction_globalsort where SEGMENT.ID in (0.1)")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), false, "Success")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Marked for Delete")
  }

  test("Compaction: delete, minor") {
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
  }

  test("Compaction: load from file dictory, three csv file, major") {
    for (i <- 0 until 6) {
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE compaction_globalsort")
    }
    sql("alter table compaction_globalsort set tblproperties('global_sort_partitions'='2')")
    sql("ALTER TABLE compaction_globalsort COMPACT 'MAJOR'")

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 2)
    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(72)))
    checkAnswer(sql("SELECT * FROM compaction_globalsort order by name, id"),
      sql("SELECT * FROM carbon_localsort order by name, id"))
    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Success")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")
  }

  test("Compaction: load from file dictory, three csv file, minor") {
    for (i <- 0 until 6) {
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localsort")
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE compaction_globalsort")
    }
    sql("alter table compaction_globalsort set tblproperties('global_sort_partitions'='2')")
    sql("ALTER TABLE compaction_globalsort COMPACT 'MINOR'")

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 2)
    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(72)))
    checkAnswer(sql("SELECT * FROM compaction_globalsort order by name, id"),
      sql("SELECT * FROM carbon_localsort order by name, id"))
    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Success")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")
  }

  test("Compaction: one file and no sort_columns") {
    sql("DROP TABLE IF EXISTS compaction_globalsort2")
    sql(
      """
        | CREATE TABLE compaction_globalsort2(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        |  TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort2")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort2")
    sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort2")

    sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort")

    sql("alter table compaction_globalsort set tblproperties('global_sort_partitions'='1')")
    sql("ALTER TABLE compaction_globalsort COMPACT 'MAJOR'")
    sql("clean files for table compaction_globalsort")

    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    val status = segments.collect().map { each => (each.toSeq) (1) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 1)
    assert(status.filter(_.equals("Compacted")).length == 0)

    assert(getIndexFileCount("compaction_globalsort", "0.1") === 1)
    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort"), Seq(Row(12)))
    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort2"), Seq(Row(12)))
    checkAnswer(sql("SELECT * FROM compaction_globalsort"),
      sql("SELECT * FROM compaction_globalsort2"))
    sql("DROP TABLE IF EXISTS compaction_globalsort2")
  }

  test("Compaction: global_sort sort_columns is int data type") {
    sql("DROP TABLE IF EXISTS compaction_globalsort2")
    sql(
      """
        | CREATE TABLE compaction_globalsort2(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        |  TBLPROPERTIES('SORT_COLUMNS'='id','SORT_SCOPE'='GLOBAL_SORT', 'global_sort_partitions'='1')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort2")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort2")
    sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort2")

    sql("ALTER TABLE compaction_globalsort2 COMPACT 'MAJOR'")
    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort2")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    val status = segments.collect().map { each => (each.toSeq) (1) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 4)
    assert(status.filter(_.equals("Compacted")).length == 3)

    assert(getIndexFileCount("compaction_globalsort2", "0.1") === 1)
    checkAnswer(sql("SELECT COUNT(*) FROM compaction_globalsort2"), Seq(Row(12)))
    sql("DROP TABLE IF EXISTS compaction_globalsort2")
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

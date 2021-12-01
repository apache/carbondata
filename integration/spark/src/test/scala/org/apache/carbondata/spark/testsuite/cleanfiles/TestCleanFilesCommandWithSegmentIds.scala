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

package org.apache.carbondata.spark.testsuite.cleanfiles

import java.io.{File, IOException}

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.util.CommonUtil

class TestCleanFilesCommandWithSegmentIds extends QueryTest with BeforeAndAfterEach {
  override protected def beforeEach(): Unit = {
    sql(
      """
        | CREATE TABLE if not exists cleantest (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int)
        | STORED AS carbondata
      """.stripMargin)
    loadData()
  }

  test("Test clean files after delete command") {
    sql(s"""Delete from table cleantest where segment.id in(0, 1, 2, 3)""")
    var showSegments = sql("show segments for table cleantest").collect().map {
      a => (a.get(0), a.get(1))
    }
    assert(showSegments.length == 4)
    assert(showSegments.count(_._2 == "Marked for Delete") == 4)
    sql("CLEAN FILES FOR TABLE cleantest OPTIONS('segment_ids'='0, 1,\t2,3')")
    showSegments = sql("show segments for table cleantest").collect().map {
      a => (a.get(0), a.get(1))
    }
    assert(showSegments.isEmpty)
    showSegments = sql("show history segments for table cleantest").collect().map {
      a => (a.get(0), a.get(1))
    }
    assert(showSegments.length == 4)
    assert(showSegments.count(_._2 == "Marked for Delete") == 4)

    // check all segments data are deleted successfully
    val table = CarbonEnv.getCarbonTable(None, "cleantest")(sqlContext.sparkSession)
    val path = CarbonTablePath.getPartitionDir(table.getTablePath)
    val segmentFolders = new File(path).listFiles()
    assert(segmentFolders.isEmpty)
  }

  test("Test clean files after delete command with empty segment_ids option") {
    sql(s"""Delete from table cleantest where segment.id in(0, 1, 2, 3)""")
    var showSegments = sql("show segments for table cleantest").collect().map {
      a => (a.get(0), a.get(1))
    }
    assert(showSegments.length == 4)
    assert(showSegments.count(_._2 == "Marked for Delete") == 4)
    sql("CLEAN FILES FOR TABLE cleantest OPTIONS('segment_ids'='')")
    showSegments = sql("show segments for table cleantest").collect().map {
      a => (a.get(0), a.get(1))
    }
    assert(showSegments.length == 4)
    assert(showSegments.count(_._2 == "Marked for Delete") == 4)

    // check no segment data are deleted
    val table = CarbonEnv.getCarbonTable(None, "cleantest")(sqlContext.sparkSession)
    val path = CarbonTablePath.getPartitionDir(table.getTablePath)
    val segmentFolders = new File(path).listFiles()
    assert(segmentFolders.length == 4)
  }

  test("Test clean files after compaction command with segment_ids option") {
    sql(s"""alter table cleantest compact 'custom' where segment.id in(0, 1, 2, 3)""")
    var showSegments = sql("show segments for table cleantest").collect().map {
      a => (a.get(0), a.get(1))
    }
    assert(showSegments.length == 5)
    assert(showSegments.count(_._2 == "Compacted") == 4)
    assert(showSegments.count(_._2 == "Success") == 1)
    sql("CLEAN FILES FOR TABLE cleantest OPTIONS('segment_ids'='0, 1,\t2,3')")
    showSegments = sql("show segments for table cleantest").collect().map {
      a => (a.get(0), a.get(1))
    }
    assert(showSegments.length == 1)

    // check all segments data are deleted successfully
    val table = CarbonEnv.getCarbonTable(None, "cleantest")(sqlContext.sparkSession)
    val path = CarbonTablePath.getPartitionDir(table.getTablePath)
    val segmentFolders = new File(path).listFiles()
    assert(segmentFolders.length == 1)
    assert(segmentFolders.head.getName.contains("Segment_0.1"))
  }

  test("Test clean files after compaction command with segment_ids option with 1 segment") {
    sql(s"""alter table cleantest compact 'custom' where segment.id in(0, 1, 2, 3)""")
    var showSegments = sql("show segments for table cleantest").collect().map {
      a => (a.get(0), a.get(1))
    }
    assert(showSegments.length == 5)
    assert(showSegments.count(_._2 == "Compacted") == 4)
    assert(showSegments.count(_._2 == "Success") == 1)
    sql("CLEAN FILES FOR TABLE cleantest OPTIONS('segment_ids'='0')")
    showSegments = sql("show segments for table cleantest").collect().map {
      a => (a.get(0), a.get(1))
    }
    assert(showSegments.length == 4)

    // check all segments data are deleted successfully
    val table = CarbonEnv.getCarbonTable(None, "cleantest")(sqlContext.sparkSession)
    val path = CarbonTablePath.getPartitionDir(table.getTablePath)
    val segmentFolders = new File(path).listFiles()
    assert(segmentFolders.length == 4)
  }

  test("Test clean files after compaction command without segment_ids option") {
    sql(s"""alter table cleantest compact 'custom' where segment.id in(0, 1, 2, 3)""")
    var showSegments = sql("show segments for table cleantest").collect().map {
      a => (a.get(0), a.get(1))
    }
    assert(showSegments.length == 5)
    assert(showSegments.count(_._2 == "Compacted") == 4)
    assert(showSegments.count(_._2 == "Success") == 1)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql("CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')")
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
    showSegments = sql("show segments for table cleantest").collect().map {
      a => (a.get(0), a.get(1))
    }
    assert(showSegments.length == 1)

    // check all segments data are deleted successfully
    val table = CarbonEnv.getCarbonTable(None, "cleantest")(sqlContext.sparkSession)
    val path = CarbonTablePath.getPartitionDir(table.getTablePath)
    val segmentFolders = new File(path).listFiles()
    assert(segmentFolders.length == 1)
    assert(segmentFolders.head.getName.contains("Segment_0.1"))
  }

  test("Test clean files on Success segment") {
    // clean files on all segments
    val ex = intercept[IOException] {
      sql("CLEAN FILES FOR TABLE cleantest OPTIONS('segment_ids'='0, 1, 2, 3')")
    }
    assert(ex.getMessage.contains(
      "Clean files request is failed for default.cleantest. Segment 0,1,2,3"))

    // check no segment data are deleted
    val table = CarbonEnv.getCarbonTable(None, "cleantest")(sqlContext.sparkSession)
    val path = CarbonTablePath.getPartitionDir(table.getTablePath)
    val segmentFolders = new File(path).listFiles()
    assert(segmentFolders.length == 4)
  }

  test("Test clean files on non exists segment") {
    // clean files on non exists segments
    val ex = intercept[IOException] {
      sql("CLEAN FILES FOR TABLE cleantest OPTIONS('segment_ids'='4')")
    }
    assert(ex.getMessage.contains(
      "Clean files request is failed for default.cleantest. Segment 4 are not exists"))
    val showSegments = sql("show segments for table cleantest").collect().map {
      a => (a.get(0), a.get(1))
    }
    assert(showSegments.length == 4)
    assert(showSegments.count(_._2 == "Success") == 4)

    // check no segment data are deleted
    val table = CarbonEnv.getCarbonTable(None, "cleantest")(sqlContext.sparkSession)
    val path = CarbonTablePath.getPartitionDir(table.getTablePath)
    val segmentFolders = new File(path).listFiles()
    assert(segmentFolders.length == 4)
  }

  test("Test clean files with incorrect segment_ids option") {
    val ex = intercept[MalformedCarbonCommandException] {
      sql("CLEAN FILES FOR TABLE cleantest OPTIONS('segment_ids'='s, d, e, f')")
    }
    assert(ex.getMessage.contains("Invalid segment id in clean files command."))
  }

  test("Test CommonUtil.isSegmentId") {
    val segmentIds = List("0", "1", "2", "0.1", "1.1")
    val nonSegmentIds = List("1.", "1..", ".0", "1..1", "a", "1.a")
    assert(segmentIds.count(CommonUtil.isSegmentId) == 5)
    assert(!nonSegmentIds.exists(CommonUtil.isSegmentId))
  }

  override protected def afterEach(): Unit = {
    sql("drop table if exists cleantest")
  }

  def loadData() : Unit = {
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE cleantest OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE cleantest OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE cleantest OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE cleantest OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
  }
}

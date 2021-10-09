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

import java.io.File

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath.DataFileUtil

class TestCleanFilesCommandPartitionTableWithSegmentIds extends QueryTest with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {
    createPartitionTable()
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
    val tableFolder = new File(table.getTablePath).listFiles()
    assert(!tableFolder.exists(f =>
      f.getName.contains("add=abc") || f.getName.contains("add=adc")))
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

    // check all segments data before compaction are deleted successfully
    val table = CarbonEnv.getCarbonTable(None, "cleantest")(sqlContext.sparkSession)
    val abcFiles = new File(
      table.getTablePath + CarbonCommonConstants.FILE_SEPARATOR + "add=abc").listFiles()
    val adcFiles = new File(
      table.getTablePath + CarbonCommonConstants.FILE_SEPARATOR + "add=adc").listFiles()
    for (file <- abcFiles) {
      if (file.getPath.endsWith("carbondata")) {
        assert(DataFileUtil.getSegmentNo(file.getName).equals("0.1"))
      }
    }
    for (file <- adcFiles) {
      if (file.getPath.endsWith("carbondata")) {
        assert(DataFileUtil.getSegmentNo(file.getName).equals("0.1"))
      }
    }
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

    // check all segments data before compaction are deleted successfully
    val table = CarbonEnv.getCarbonTable(None, "cleantest")(sqlContext.sparkSession)
    val abcFiles = new File(
      table.getTablePath + CarbonCommonConstants.FILE_SEPARATOR + "add=abc").listFiles()
    val adcFiles = new File(
      table.getTablePath + CarbonCommonConstants.FILE_SEPARATOR + "add=adc").listFiles()
    for (file <- abcFiles) {
      if (file.getPath.endsWith("carbondata")) {
        assert(DataFileUtil.getSegmentNo(file.getName).equals("0.1"))
      }
    }
    for (file <- adcFiles) {
      if (file.getPath.endsWith("carbondata")) {
        assert(DataFileUtil.getSegmentNo(file.getName).equals("0.1"))
      }
    }
  }

  override protected def afterEach(): Unit = {
    sql("drop table if exists cleantest")
  }

  def createPartitionTable() : Unit = {
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql(
      """
        | CREATE TABLE CLEANTEST (id Int, id1 INT, name STRING ) PARTITIONED BY (add String)
        | STORED AS carbondata
      """.stripMargin)
  }

  def loadData() : Unit = {
    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"bob","abc"""")
    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"jack","abc"""")
    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"johnny","adc"""")
    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"Reddit","adc"""")
  }
}

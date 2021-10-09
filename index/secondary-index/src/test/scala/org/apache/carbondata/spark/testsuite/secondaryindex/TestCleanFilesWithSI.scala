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
package org.apache.carbondata.spark.testsuite.secondaryindex

import java.io.File

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath


/**
 * test cases for testing clean files command on table with SI
 */
class TestCleanFilesWithSI extends QueryTest with BeforeAndAfterEach {

  var mainTable : CarbonTable = _
  var mainTablePath : String = _
  var indexTable : CarbonTable = _
  var indexTablePath : String = _

  override protected def beforeEach(): Unit = {
    sql("drop table if exists test.maintable")
    sql("drop database if exists test cascade")
    sql("create database test")
    sql("CREATE TABLE test.maintable(a INT, b STRING, c STRING) stored as carbondata")
    sql("CREATE INDEX indextable1 on table test.maintable(c) as 'carbondata'")
    sql("INSERT INTO test.maintable SELECT 1,'string1', 'string2'")
    sql("INSERT INTO test.maintable SELECT 1,'string1', 'string2'")
    sql("INSERT INTO test.maintable SELECT 1,'string1', 'string2'")
    sql("DELETE FROM TABLE test.maintable WHERE SEGMENT.ID IN(0,1,2)")

    mainTable =
      CarbonEnv.getCarbonTable(Option("test"), "maintable")(sqlContext.sparkSession)
    mainTablePath =
      CarbonTablePath.getPartitionDir(mainTable.getTablePath)
    indexTable =
      CarbonEnv.getCarbonTable(Option("test"), "indextable1")(sqlContext.sparkSession)
    indexTablePath =
      CarbonTablePath.getPartitionDir(indexTable.getTablePath)
  }

  test("Test clean files on table with secondary index and si missing segment 0") {
    // simulate main table and SI table segment inconsistent scenario
    sql("CLEAN FILES FOR TABLE test.indextable1 options('segment_ids'='0')")
    sql("CLEAN FILES FOR TABLE test.maintable options('segment_ids'='0,1')")

    var mainTableSegmentCount = sql("SHOW SEGMENTS FOR TABLE test.maintable").count()
    // main table segment 2
    assert(mainTableSegmentCount == 1)
    var siTableSegmentCount = sql("SHOW SEGMENTS FOR TABLE test.indextable1").count()
    // si table segment 2
    assert(siTableSegmentCount == 1)

    var segmentFolders = new File(mainTablePath).listFiles()
    assert(segmentFolders.length == 1)
    segmentFolders = new File(indexTablePath).listFiles()
    assert(segmentFolders.length == 1)

    sql("CLEAN FILES FOR TABLE test.maintable options('segment_ids'='2')")
    mainTableSegmentCount = sql("SHOW SEGMENTS FOR TABLE test.maintable").count()
    // main table has no segment left
    assert(mainTableSegmentCount == 0)
    siTableSegmentCount = sql("SHOW SEGMENTS FOR TABLE test.indextable1").count()
    // si table has no segment left
    assert(siTableSegmentCount == 0)

    segmentFolders = new File(mainTablePath).listFiles()
    assert(segmentFolders.length == 0)
    segmentFolders = new File(indexTablePath).listFiles()
    assert(segmentFolders.length == 0)
  }

  test("Test clean files on table with secondary index and si missing segment 1") {
    // simulate main table and SI table segment inconsistent scenario
    sql("CLEAN FILES FOR TABLE test.indextable1 options('segment_ids'='1')")
    sql("CLEAN FILES FOR TABLE test.maintable options('segment_ids'='0,1')")

    var mainTableSegmentCount = sql("SHOW SEGMENTS FOR TABLE test.maintable").count()
    // main table segment 2
    assert(mainTableSegmentCount == 1)
    var siTableSegmentCount = sql("SHOW SEGMENTS FOR TABLE test.indextable1").count()
    // si table segment 2
    assert(siTableSegmentCount == 1)

    var segmentFolders = new File(mainTablePath).listFiles()
    assert(segmentFolders.length == 1)
    segmentFolders = new File(indexTablePath).listFiles()
    assert(segmentFolders.length == 1)

    sql("CLEAN FILES FOR TABLE test.maintable options('segment_ids'='2')")
    mainTableSegmentCount = sql("SHOW SEGMENTS FOR TABLE test.maintable").count()
    // main table has no segment left
    assert(mainTableSegmentCount == 0)
    siTableSegmentCount = sql("SHOW SEGMENTS FOR TABLE test.indextable1").count()
    // si table has no segment left
    assert(siTableSegmentCount == 0)

    segmentFolders = new File(mainTablePath).listFiles()
    assert(segmentFolders.length == 0)
    segmentFolders = new File(indexTablePath).listFiles()
    assert(segmentFolders.length == 0)
  }

  test("Test clean files on table with secondary index" +
       " and si metadata and physical files is not consistent") {
    // simulate SI table metadata and physical files not consistent, remove segment 1 from si
    // tablestatus
    val siLoadDetails = SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath)
    val modifiedLoadDetails = new Array[LoadMetadataDetails](siLoadDetails.length - 1)
    modifiedLoadDetails(0) = siLoadDetails(0)
    modifiedLoadDetails(1) = siLoadDetails(2)
    SegmentStatusManager.writeLoadDetailsIntoFile(
      CarbonTablePath.getTableStatusFilePath(indexTable.getTablePath), modifiedLoadDetails)

    sql("CLEAN FILES FOR TABLE test.maintable options('segment_ids'='0,1')")
    var mainTableSegmentCount = sql("SHOW SEGMENTS FOR TABLE test.maintable").count()
    // main table segment 2
    assert(mainTableSegmentCount == 1)
    var siTableSegmentCount = sql("SHOW SEGMENTS FOR TABLE test.indextable1").count()
    // si table segment 2
    assert(siTableSegmentCount == 1)

    var segmentFolders = new File(mainTablePath).listFiles()
    assert(segmentFolders.length == 1)
    segmentFolders = new File(indexTablePath).listFiles()
    // si table Segment_1 folder is not cleaned because segment 1 is not presented in tablestatus
    assert(segmentFolders.length == 2)

    sql("CLEAN FILES FOR TABLE test.maintable options('segment_ids'='2')")
    mainTableSegmentCount = sql("SHOW SEGMENTS FOR TABLE test.maintable").count()
    // main table has no segment left
    assert(mainTableSegmentCount == 0)
    siTableSegmentCount = sql("SHOW SEGMENTS FOR TABLE test.indextable1").count()
    // si table has no segment left
    assert(siTableSegmentCount == 0)

    segmentFolders = new File(mainTablePath).listFiles()
    assert(segmentFolders.length == 0)
    segmentFolders = new File(indexTablePath).listFiles()
    // si table Segment_1 folder is not cleaned because segment 1 is not presented in tablestatus
    assert(segmentFolders.length == 1)
    assert(segmentFolders.head.getName.equalsIgnoreCase("Segment_1"))
  }
}

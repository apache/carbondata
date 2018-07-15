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
package org.apache.carbondata.spark.testsuite.datacompaction

import scala.collection.JavaConverters._

import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.statusmanager.{SegmentManager, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
  * FT for compaction scenario where major segment should not be included in minor.
  */
class MajorCompactionIgnoreInMinorTest extends QueryTest with BeforeAndAfterAll {

  val csvFilePath1 = s"$resourcesPath/compaction/compaction1.csv"
  val csvFilePath2 = s"$resourcesPath/compaction/compaction2.csv"
  val csvFilePath3 = s"$resourcesPath/compaction/compaction3.csv"

  override def beforeAll {
  }

  def createTableAndLoadData(): Unit = {
    CarbonProperties.getInstance().addProperty("carbon.compaction.level.threshold", "2,2")
    sql("drop table if exists  ignoremajor")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")
    sql(
      "CREATE TABLE IF NOT EXISTS ignoremajor (country String, ID Int, date Timestamp, name " +
        "String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
        ".format'"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE ignoremajor OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE ignoremajor  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    // compaction will happen here.
    sql("alter table ignoremajor compact 'major'")

    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE ignoremajor OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE ignoremajor  OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("alter table ignoremajor compact 'minor'"
    )
  }

  /**
    * Delete should not work on compacted segment.
    */
  test("delete compacted segment and check status") {
    createTableAndLoadData()
    intercept[Throwable] {
      sql("delete from table ignoremajor where segment.id in (2)")
    }

    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "ignoremajor"
    )

    val carbonTablePath = carbonTable.getMetadataPath
    val segs = SegmentStatusManager.readLoadMetadata(carbonTablePath)

    // status should remain as compacted.
    assertResult(SegmentStatus.COMPACTED)(segs(3).getSegmentStatus)
  }

  /**
    * Delete should not work on compacted segment.
    */
  test("delete compacted segment by date and check status") {
    createTableAndLoadData()
    sql(
      "delete from table ignoremajor where segment.starttime before " +
        " '2222-01-01 19:35:01'"
    )
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "ignoremajor"
    )
    val carbontablePath = carbonTable.getMetadataPath
    val segs = SegmentStatusManager.readLoadMetadata(carbontablePath)

    // status should remain as compacted for segment 2.
    assertResult(SegmentStatus.COMPACTED)(segs(3).getSegmentStatus)
    // for segment 0.1 . should get deleted
    assertResult(SegmentStatus.MARKED_FOR_DELETE)(segs(2).getSegmentStatus)
  }

  /**
    * Test whether major compaction is not included in minor compaction.
    */
  test("delete merged folder and check segments") {
    createTableAndLoadData()
    // delete merged segments
    sql("clean files for table ignoremajor")

    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "ignoremajor"
    )
    val absoluteTableIdentifier = carbonTable
      .getAbsoluteTableIdentifier
    val segmentStatusManager = new SegmentManager()

    // merged segment should not be there
    val segments = segmentStatusManager.getValidSegments(absoluteTableIdentifier).getValidSegments.asScala.map(_.getSegmentNo).toList
    assert(segments.contains("0.1"))
    assert(segments.contains("2.1"))
    assert(!segments.contains("2"))
    assert(!segments.contains("3"))
  }

  /**
   * Execute two major compactions sequentially
   */
  test("Execute two major compactions sequentially") {
    sql("drop table if exists testmajor")
    sql(
      "CREATE TABLE IF NOT EXISTS testmajor (country String, ID Int, date Timestamp, name " +
      "String, " +
      "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
      ".format'"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE testmajor OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE testmajor  OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    // compaction will happen here.
    sql("alter table testmajor compact 'major'")
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE testmajor OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE testmajor  OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("alter table testmajor compact 'major'")

    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "testmajor"
    )
    val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    val segmentStatusManager = new SegmentManager()

    // merged segment should not be there
    val segments = segmentStatusManager.getValidSegments(absoluteTableIdentifier).getValidSegments.asScala.map(_.getSegmentNo).toList
    assert(!segments.contains("0.1"))
    assert(segments.contains("0.2"))
    assert(!segments.contains("2"))
    assert(!segments.contains("3"))

  }

  override def afterAll {
    sql("drop table if exists  ignoremajor")
    sql("drop table if exists  testmajor")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }

}

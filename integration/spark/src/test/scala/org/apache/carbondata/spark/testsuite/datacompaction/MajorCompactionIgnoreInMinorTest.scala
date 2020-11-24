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

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonProperties

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
        "String, phonetype String, serialname String, salary Int) STORED AS carbondata"
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
    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(
      absoluteTableIdentifier)

    // merged segment should not be there
    val segments = segmentStatusManager
      .getValidAndInvalidSegments
      .getValidSegments
      .asScala
      .map(_.getSegmentNo)
      .toList
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
      "String, phonetype String, serialname String, salary Int) STORED AS carbondata"
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
    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(
      absoluteTableIdentifier)

    // merged segment should not be there
    val segments = segmentStatusManager
      .getValidAndInvalidSegments
      .getValidSegments
      .asScala
      .map(_.getSegmentNo)
      .toList
    assert(!segments.contains("0.1"))
    assert(segments.contains("0.2"))
    assert(!segments.contains("2"))
    assert(!segments.contains("3"))

  }

  def generateData(numOrders: Int = 100000): DataFrame = {
    import sqlContext.implicits._
    sqlContext.sparkContext.parallelize(1 to numOrders, 4)
      .map { x => ("country" + x, x, "07/23/2015", "name" + x, "phonetype" + x % 10,
        "serialname" + x, x + 10000)
      }.toDF("country", "ID", "date", "name", "phonetype", "serialname", "salary")
  }

  test("test skip segment whose data size exceed threshold in minor compaction " +
    "in system level control and table level") {
    CarbonProperties.getInstance().addProperty("carbon.compaction.level.threshold", "2,2")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")
    // set threshold to 1MB in system level
    CarbonProperties.getInstance().addProperty("carbon.minor.compaction.size", "1")

    sql("drop table if exists  minor_threshold")
    sql("drop table if exists  tmp")
    sql(
      "CREATE TABLE IF NOT EXISTS minor_threshold (country String, ID Int, date" +
        " Timestamp, name String, phonetype String, serialname String, salary Int) " +
        "STORED AS carbondata"
    )
    sql(
      "CREATE TABLE IF NOT EXISTS tmp (country String, ID Int, date Timestamp," +
        " name String, phonetype String, serialname String, salary Int) STORED AS carbondata"
    )
    val initframe = generateData(100000)
    initframe.write
      .format("carbondata")
      .option("tablename", "tmp")
      .mode(SaveMode.Overwrite)
      .save()
    // load 3 segments
    for (i <- 0 to 2) {
      sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE minor_threshold" +
        " OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
      )
    }
    // insert a new segment(id is 3) data size exceed 1 MB
    sql("insert into minor_threshold select * from tmp")
    // load another 3 segments
    for (i <- 0 to 2) {
      sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE minor_threshold" +
        " OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
      )
    }
    // do minor compaction
    sql("alter table minor_threshold compact 'minor'")
    // check segment 3 whose size exceed the limit should not be compacted but success
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME, "minor_threshold")
    val carbonTablePath = carbonTable.getMetadataPath
    val segments = SegmentStatusManager.readLoadMetadata(carbonTablePath);
    assertResult(SegmentStatus.SUCCESS)(segments(3).getSegmentStatus)
    assertResult(100030)(sql("select count(*) from minor_threshold").collect().head.get(0))

    // change the threshold to 5MB by dynamic table properties setting, then the segment whose id is
    // 3 should be included in minor compaction
    sql("alter table minor_threshold set TBLPROPERTIES('minor_compaction_size'='5')")
    // reload some segments
    for (i <- 0 to 2) {
      sql("insert into minor_threshold select * from tmp")
    }
    // do minor compaction
    sql("alter table minor_threshold compact 'minor'")
    // check segment 3 whose size not exceed the new threshold limit should be compacted now
    val segments2 = SegmentStatusManager.readLoadMetadata(carbonTablePath);
    assertResult(SegmentStatus.COMPACTED)(segments2(3).getSegmentStatus)
    assertResult(400030)(sql("select count(*) from minor_threshold").collect().head.get(0))

    // reset the properties
    CarbonProperties.getInstance().addProperty("carbon.minor.compaction.size", "-1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

  test("test skip segment whose data size exceed threshold in minor compaction " +
    "for partition table") {
    CarbonProperties.getInstance().addProperty("carbon.compaction.level.threshold", "2,2")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")

    sql("drop table if exists  tmp")
    sql("drop table if exists  minor_threshold_partition")
    sql(
      "CREATE TABLE IF NOT EXISTS tmp (country String, ID Int, date Timestamp," +
        " name String, phonetype String, serialname String, salary Int) STORED AS carbondata"
    )
    val initframe = generateData(100000)
    initframe.write
      .format("carbondata")
      .option("tablename", "tmp")
      .mode(SaveMode.Overwrite)
      .save()
    // set threshold to 1MB for partition table
    sql(
      "CREATE TABLE IF NOT EXISTS minor_threshold_partition (country String, ID Int," +
        " date Timestamp, name String, serialname String, salary Int) PARTITIONED BY " +
        "(phonetype string) STORED AS carbondata TBLPROPERTIES('minor_compaction_size'='1')"
    )
    // load 3 segments
    for (i <- 0 to 2) {
      sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE minor_threshold_partition" +
        " OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
      )
    }
    // insert a new segment(id is 3) data size exceed 1 MB
    sql("insert into minor_threshold_partition select country, ID, date, name, serialname," +
      " salary, phonetype from tmp")
    // load another 3 segments
    for (i <- 0 to 2) {
      sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE minor_threshold_partition" +
        " OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
      )
    }
    // do minor compaction for minor_threshold_partition
    sql("alter table minor_threshold_partition compact 'minor'")
    // check segment 3 whose size exceed the limit should not be compacted
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME, "minor_threshold_partition")
    val carbonTablePath2 = carbonTable.getMetadataPath
    val segments = SegmentStatusManager.readLoadMetadata(carbonTablePath2);
    assertResult(SegmentStatus.SUCCESS)(segments(3).getSegmentStatus)
    assertResult(100030)(sql("select count(*) from " +
      "minor_threshold_partition").collect().head.get(0))
    // reset the properties
    CarbonProperties.getInstance().addProperty("carbon.minor.compaction.size", "-1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists  ignoremajor")
    sql("drop table if exists  testmajor")
  }

}

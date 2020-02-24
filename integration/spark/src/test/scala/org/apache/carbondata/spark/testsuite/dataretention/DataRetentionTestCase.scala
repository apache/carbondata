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

package org.apache.carbondata.spark.testsuite.dataretention

import java.text.SimpleDateFormat

import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.core.locks.{CarbonLockFactory, ICarbonLock, LockUsage}
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.{CarbonEnv, Row}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

/**
 * This class contains data retention feature test cases
 */
class DataRetentionTestCase extends QueryTest with BeforeAndAfterAll {

  var absoluteTableIdentifierForLock: AbsoluteTableIdentifier = null
  var absoluteTableIdentifierForRetention: AbsoluteTableIdentifier = null
  var carbonTablePath : String = null
  var carbonDateFormat = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP)
  var defaultDateFormat = new SimpleDateFormat(CarbonCommonConstants
    .CARBON_TIMESTAMP_DEFAULT_FORMAT)
  var carbonTableStatusLock: ICarbonLock = null
  var carbonDeleteSegmentLock: ICarbonLock = null
  var carbonCleanFilesLock: ICarbonLock = null
  var carbonMetadataLock: ICarbonLock = null

  override def beforeAll {
    sql("drop table if exists DataRetentionTable")
    sql("drop table if exists retentionlock")

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CARBON_LOCK, "1")
    CarbonProperties.getInstance.addProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME, "1")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "CREATE table DataRetentionTable (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) STORED AS carbondata"

    )
    sql(
      "CREATE table retentionlock (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) STORED AS carbondata"

    )
    val carbonTable =
      CarbonEnv.getCarbonTable(Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
        "retentionlock")(sqlContext.sparkSession)
    absoluteTableIdentifierForLock = carbonTable.getAbsoluteTableIdentifier
    val carbonTable2 =
      CarbonEnv.getCarbonTable(Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
        "dataRetentionTable")(sqlContext.sparkSession)
    absoluteTableIdentifierForRetention = carbonTable2.getAbsoluteTableIdentifier
    carbonTablePath = CarbonTablePath
      .getMetadataPath(absoluteTableIdentifierForRetention.getTablePath)
    carbonTableStatusLock = CarbonLockFactory
      .getCarbonLockObj(absoluteTableIdentifierForLock, LockUsage.TABLE_STATUS_LOCK)
    carbonDeleteSegmentLock= CarbonLockFactory
      .getCarbonLockObj(absoluteTableIdentifierForLock, LockUsage.DELETE_SEGMENT_LOCK)
    carbonCleanFilesLock = CarbonLockFactory
      .getCarbonLockObj(absoluteTableIdentifierForLock, LockUsage.CLEAN_FILES_LOCK)
    carbonMetadataLock = CarbonLockFactory
      .getCarbonLockObj(absoluteTableIdentifierForLock, LockUsage.METADATA_LOCK)
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE retentionlock " +
      "OPTIONS('DELIMITER' =  ',')")

    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE DataRetentionTable " +
      "OPTIONS('DELIMITER' =  ',')")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention2.csv' INTO TABLE DataRetentionTable " +
      "OPTIONS('DELIMITER' = ',')")

  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists DataRetentionTable")
    sql("drop table if exists retentionlock")
  }


  private def getSegmentStartTime(segments: Array[LoadMetadataDetails],
      segmentId: Integer): String = {
    val segmentLoadTimeString = segments(segmentId).getLoadStartTime()
    var loadTime = carbonDateFormat.parse(carbonDateFormat.format(segmentLoadTimeString))
    // add one min to execute delete before load start time command
    loadTime = DateUtils.addMinutes(loadTime, 1)
    defaultDateFormat.format(loadTime)

  }


  test("RetentionTest_withoutDelete") {
    checkAnswer(
      sql("SELECT country, count(salary) AS amount FROM DataRetentionTable WHERE country" +
          " IN ('china','ind','aus','eng') GROUP BY country"
      ),
      Seq(Row("aus", 9), Row("ind", 9))
    )
  }

  test("RetentionTest_DeleteSegmentsByLoadTime") {
    val segments: Array[LoadMetadataDetails] =
      SegmentStatusManager.readLoadMetadata(carbonTablePath)
    // check segment length, it should be 3 (loads)
    if (segments.length != 2) {
      assert(false)
    }

    val actualValue: String = getSegmentStartTime(segments, 1)
    // delete segments (0,1) which contains ind, aus
    sql(
      "delete from table DataRetentionTable where segment.starttime before '" + actualValue + "'")

    // load segment 2 which contains eng
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention3.csv' INTO TABLE DataRetentionTable " +
      "OPTIONS('DELIMITER' = ',')")
    checkAnswer(
      sql("SELECT country, count(salary) AS amount FROM DataRetentionTable WHERE country" +
          " IN ('china','ind','aus','eng') GROUP BY country"
      ),
      Seq(Row("eng", 9))
    )
  }

  test("RetentionTest3_DeleteByLoadId") {
    // delete segment 2 and load ind segment
    sql("delete from table DataRetentionTable where segment.id in (2)")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE DataRetentionTable " +
      "OPTIONS('DELIMITER' = ',')")
    checkAnswer(
      sql("SELECT country, count(salary) AS amount FROM DataRetentionTable WHERE country" +
          " IN ('china','ind','aus','eng') GROUP BY country"
      ),
      Seq(Row("ind", 9))
    )

    // these queries should execute without any error.
    sql("show segments for table DataRetentionTable")
    sql("clean files for table DataRetentionTable")
  }

  test("RetentionTest4_DeleteByInvalidLoadId") {
    val e = intercept[MalformedCarbonCommandException] {
      // delete segment with no id
      sql("delete from table DataRetentionTable where segment.id in ()")
    }
    assert(e.getMessage.contains("should not be empty"))
  }

  test("test delete segments by load date with case-insensitive table name") {
    sql(
      """
      CREATE TABLE IF NOT EXISTS carbon_table_1
      (ID Int, date Timestamp, country String,
      name String, phonetype String, serialname String, salary Int)
      STORED AS carbondata
      """)

    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/emptyDimensionData.csv' into table carbon_table_1")

    checkAnswer(
      sql("select count(*) from carbon_table_1"), Seq(Row(20)))

    sql("delete from table carbon_table_1 where segment.starttime " +
        " before '2099-07-28 11:00:00'")

    checkAnswer(
      sql("select count(*) from carbon_table_1"), Seq(Row(0)))

    sql("DROP TABLE carbon_table_1")
  }

  test("RetentionTest_DeleteSegmentsByLoadTimeValiadtion") {

    val e = intercept[MalformedCarbonCommandException] {
      sql(
        "delete from table DataRetentionTable where segment.starttime before" +
        " 'abcd-01-01 00:00:00'")
    }
    assert(e.getMessage.contains("Invalid load start time format"))

    val ex = intercept[MalformedCarbonCommandException] {
      sql(
        "delete from table DataRetentionTable where segment.starttime before" +
        " '2099:01:01 00:00:00'")
    }
    assert(ex.getMessage.contains("Invalid load start time format"))

    checkAnswer(
      sql("SELECT country, count(salary) AS amount FROM DataRetentionTable WHERE country" +
          " IN ('china','ind','aus','eng') GROUP BY country"
      ),
      Seq(Row("ind", 9))
    )
    sql("delete from table DataRetentionTable where segment.starttime before '2099-01-01'")
    checkAnswer(
      sql("SELECT country, count(salary) AS amount FROM DataRetentionTable WHERE country" +
          " IN ('china','ind','aus','eng') GROUP BY country"), Seq())


  }

  test("RetentionTest_InvalidDeleteCommands") {
    // All these queries should fail.
    intercept[Exception] {
      sql("DELETE LOADS FROM TABLE DataRetentionTable where STARTTIME before '2099-01-01'")
    }

    intercept[Exception] {
      sql("DELETE LOAD 2 FROM TABLE DataRetentionTable")
    }

    intercept[Exception] {
      sql("show loads for table DataRetentionTable")
    }

  }

  test("RetentionTest_Locks") {

    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE retentionlock " +
      "OPTIONS('DELIMITER' = ',')")
    carbonDeleteSegmentLock.lockWithRetries()
    carbonTableStatusLock.lockWithRetries()
    carbonCleanFilesLock.lockWithRetries()

    // delete segment 0 it should fail
    intercept[Exception] {
      sql("delete from table retentionlock where segment.id in (0)")
    }

    // it should fail
    intercept[Exception] {
      sql("delete from table retentionlock where segment.starttime before " +
          "'2099-01-01 00:00:00.0'")
    }

    // it should fail
    intercept[Exception] {
      sql("clean files for table retentionlock")
    }

    sql("SHOW SEGMENTS FOR TABLE retentionlock").show
    carbonTableStatusLock.unlock()
    carbonCleanFilesLock.unlock()
    carbonDeleteSegmentLock.unlock()

    sql("delete from table retentionlock where segment.id in (0)")
    //load and delete should execute parallely
    carbonMetadataLock.lockWithRetries()
    sql("delete from table retentionlock where segment.id in (1)")
    carbonMetadataLock.unlock()
  }
}

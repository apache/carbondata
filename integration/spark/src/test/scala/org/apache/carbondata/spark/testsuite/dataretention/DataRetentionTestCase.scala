/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.testsuite.dataretention

import java.io.File
import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.carbon.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.load.LoadMetadataDetails
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.lcm.locks.{CarbonLockFactory, ICarbonLock, LockUsage}
import org.apache.carbondata.lcm.status.SegmentStatusManager
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

/**
 * This class contains data retention test cases
 * Created by Manohar on 5/9/2016.
 */
class DataRetentionTestCase extends QueryTest with BeforeAndAfterAll {

  val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
    .getCanonicalPath
  val resource = currentDirectory + "/src/test/resources/"

  val storeLocation = new File(this.getClass.getResource("/").getPath + "/../test").getCanonicalPath
  val absoluteTableIdentifierForLock: AbsoluteTableIdentifier = new
      AbsoluteTableIdentifier(storeLocation,
        new CarbonTableIdentifier(CarbonCommonConstants.DATABASE_DEFAULT_NAME, "retentionlock", "200"))
  val absoluteTableIdentifierForRetention: AbsoluteTableIdentifier = new
      AbsoluteTableIdentifier(storeLocation,
        new CarbonTableIdentifier(
          CarbonCommonConstants.DATABASE_DEFAULT_NAME, "DataRetentionTable".toLowerCase(), "300"))
  val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(absoluteTableIdentifierForRetention)
  val carbonTablePath = CarbonStorePath
    .getCarbonTablePath(absoluteTableIdentifierForRetention.getStorePath,
      absoluteTableIdentifierForRetention.getCarbonTableIdentifier).getMetadataDirectoryPath

  var carbonDateFormat = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP)
  var defaultDateFormat = new SimpleDateFormat(CarbonCommonConstants
    .CARBON_TIMESTAMP_DEFAULT_FORMAT)
  val carbonTableStatusLock: ICarbonLock = CarbonLockFactory
    .getCarbonLockObj(absoluteTableIdentifierForLock.getCarbonTableIdentifier, LockUsage.TABLE_STATUS_LOCK)
  val carbonDeleteSegmentLock: ICarbonLock = CarbonLockFactory
    .getCarbonLockObj(absoluteTableIdentifierForLock.getCarbonTableIdentifier, LockUsage.DELETE_SEGMENT_LOCK)
  val carbonCleanFilesLock: ICarbonLock = CarbonLockFactory
    .getCarbonLockObj(absoluteTableIdentifierForLock.getCarbonTableIdentifier, LockUsage.CLEAN_FILES_LOCK)
  val carbonMetadataLock: ICarbonLock = CarbonLockFactory
    .getCarbonLockObj(absoluteTableIdentifierForLock.getCarbonTableIdentifier, LockUsage.METADATA_LOCK)


  override def beforeAll {
    CarbonProperties.getInstance.addProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME, "1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "CREATE table DataRetentionTable (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) stored by 'org.apache.carbondata.format'"

    )
    sql(
      "CREATE table retentionlock (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) stored by 'org.apache.carbondata.format'"

    )

    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE retentionlock " +
      "OPTIONS('DELIMITER' =  ',')")

    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE DataRetentionTable " +
      "OPTIONS('DELIMITER' =  ',')")
    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention2.csv' INTO TABLE DataRetentionTable " +
      "OPTIONS('DELIMITER' = ',')")

  }

  override def afterAll {
    sql("drop table DataRetentionTable")
    sql("drop table carbon_TABLE_1")
    sql("drop table retentionlock")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }


  private def getSegmentStartTime(segments: Array[LoadMetadataDetails],
      segmentId: Integer): String = {
    val segmentLoadTimeString = segments(segmentId).getLoadStartTime()
    var loadTime = carbonDateFormat.parse(segmentLoadTimeString)
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
    val segments: Array[LoadMetadataDetails] = segmentStatusManager
      .readLoadMetadata(carbonTablePath)
    // check segment length, it should be 3 (loads)
    if (segments.length != 2) {
      assert(false)
    }

    val actualValue: String = getSegmentStartTime(segments, 1)
    // delete segments (0,1) which contains ind, aus
    sql(
      "DELETE SEGMENTS FROM TABLE DataRetentionTable where STARTTIME before '" + actualValue + "'")

    // load segment 2 which contains eng
    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention3.csv' INTO TABLE DataRetentionTable " +
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
    sql("DELETE SEGMENT 2 FROM TABLE DataRetentionTable")
    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE DataRetentionTable " +
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
    try {
      // delete segment with no id
      sql("DELETE SEGMENT FROM TABLE DataRetentionTable")
      assert(false)
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.contains("should not be empty"))
      case _: Throwable => assert(false)
    }
  }

  test("test delete segments by load date with case-insensitive table name") {
    sql(
      """
      CREATE TABLE IF NOT EXISTS carbon_TABLE_1
      (ID Int, date Timestamp, country String,
      name String, phonetype String, serialname String, salary Int)
      STORED BY 'org.apache.carbondata.format'
      TBLPROPERTIES('DICTIONARY_EXCLUDE'='country,phonetype,serialname',
      'DICTIONARY_INCLUDE'='ID')
      """)

    sql("LOAD DATA LOCAL INPATH '" + resource +
      "emptyDimensionData.csv' into table CarBon_tAbLE_1")

    checkAnswer(
      sql("select count(*) from cArbon_TaBlE_1"), Seq(Row(20)))

    sql("delete segments from table carbon_TABLE_1 " +
      "where starttime before '2099-07-28 11:00:00'")

    checkAnswer(
      sql("select count(*) from caRbon_TabLe_1"), Seq(Row(0)))

  }
  test("RetentionTest_DeleteSegmentsByLoadTimeValiadtion") {

    try {
      sql(
        "DELETE SEGMENTS FROM TABLE DataRetentionTable where STARTTIME before" +
        " 'abcd-01-01 00:00:00'")
      assert(false)
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.contains("Invalid load start time format"))
      case _: Throwable => assert(false)
    }

    try {
      sql(
        "DELETE SEGMENTS FROM TABLE DataRetentionTable where STARTTIME before" +
        " '2099:01:01 00:00:00'")
      assert(false)
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.contains("Invalid load start time format"))
      case _: Throwable => assert(false)
    }

    checkAnswer(
      sql("SELECT country, count(salary) AS amount FROM DataRetentionTable WHERE country" +
          " IN ('china','ind','aus','eng') GROUP BY country"
      ),
      Seq(Row("ind", 9))
    )
    sql("DELETE SEGMENTS FROM TABLE DataRetentionTable where STARTTIME before '2099-01-01'")
    checkAnswer(
      sql("SELECT country, count(salary) AS amount FROM DataRetentionTable WHERE country" +
          " IN ('china','ind','aus','eng') GROUP BY country"), Seq())


  }

  test("RetentionTest_InvalidDeleteCommands") {

    // All these queries should fail.
    try {
      sql("DELETE LOADS FROM TABLE DataRetentionTable where STARTTIME before '2099-01-01'")
      throw new MalformedCarbonCommandException("Invalid query")
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(!e.getMessage.equalsIgnoreCase("Invalid query"))
      case _: Throwable => assert(true)
    }

    try {
      sql("DELETE LOAD 2 FROM TABLE DataRetentionTable")
      throw new MalformedCarbonCommandException("Invalid query")
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(!e.getMessage.equalsIgnoreCase("Invalid query"))
      case _: Throwable => assert(true)
    }

    try {
      sql("show loads for table DataRetentionTable")
      throw new MalformedCarbonCommandException("Invalid query")
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(!e.getMessage.equalsIgnoreCase("Invalid query"))
      case _: Throwable => assert(true)
    }

  }

  test("RetentionTest_Locks") {

    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE retentionlock " +
      "OPTIONS('DELIMITER' = ',')")
    carbonDeleteSegmentLock.lockWithRetries()
    carbonTableStatusLock.lockWithRetries()
    carbonCleanFilesLock.lockWithRetries()
    // delete segment 0 it should fail
    try {
      sql("DELETE SEGMENT 0 FROM TABLE retentionlock")
      throw new MalformedCarbonCommandException("Invalid")
    } catch {
      case me: MalformedCarbonCommandException =>
        assert(false)
      case ex: Exception =>
        assert(true)
    }

    // it should fail
    try {
      sql("DELETE SEGMENTS FROM TABLE retentionlock where STARTTIME before " +
          "'2099-01-01 00:00:00.0'")
      throw new MalformedCarbonCommandException("Invalid")
    } catch {
      case me: MalformedCarbonCommandException =>
        assert(false)
      case ex: Exception =>
        assert(true)
    }

    // it should fail
    try {
      sql("clean files for table retentionlock")
      throw new MalformedCarbonCommandException("Invalid")
    } catch {
      case me: MalformedCarbonCommandException =>
        assert(false)
      case ex: Exception =>
        assert(true)
    }
    carbonTableStatusLock.unlock()
    carbonCleanFilesLock.unlock()
    carbonDeleteSegmentLock.unlock()

    sql("DELETE SEGMENT 0 FROM TABLE retentionlock")
    //load and delete should execute parallely
    carbonMetadataLock.lockWithRetries()
    sql("DELETE SEGMENT 1 FROM TABLE retentionlock")
    carbonMetadataLock.unlock()
  }
}

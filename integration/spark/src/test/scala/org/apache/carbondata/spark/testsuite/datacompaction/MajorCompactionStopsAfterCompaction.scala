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

import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties

/**
  * FT for compaction scenario where major compaction will only compact the segments which are
  * present at the time of triggering the compaction.
  */
class MajorCompactionStopsAfterCompaction extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists  stopmajor")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")
    sql(
      "CREATE TABLE IF NOT EXISTS stopmajor (country String, ID decimal(7,4), date Timestamp, name " +
        "String, phonetype String, serialname String, salary Int) STORED AS carbondata"
    )

    val csvFilePath1 = s"$resourcesPath/compaction/compaction1.csv"
    val csvFilePath2 = s"$resourcesPath/compaction/compaction2.csv"
    val csvFilePath3 = s"$resourcesPath/compaction/compaction3.csv"

    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE stopmajor OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE stopmajor  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    // compaction will happen here.
    sql("alter table stopmajor compact 'major'"
    )
    Thread.sleep(2000)
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE stopmajor OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE stopmajor  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    if (checkCompactionCompletedOrNot("0.1")) {
    }

  }

  /**
    * Check if the compaction is completed or not.
    *
    * @param requiredSeg
    * @return
    */
  def checkCompactionCompletedOrNot(requiredSeg: String): Boolean = {
    var status = false
    var noOfRetries = 0
    while (!status && noOfRetries < 10) {
      val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
        CarbonCommonConstants.DATABASE_DEFAULT_NAME,
        "stopmajor"
      )
      val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier

      val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(
        absoluteTableIdentifier)

      val segments = segmentStatusManager.getValidAndInvalidSegments.getValidSegments.asScala.toList
//      segments.foreach(seg =>
//        System.out.println( "valid segment is =" + seg)
//      )

      if (!segments.contains(requiredSeg)) {
        // wait for 2 seconds for compaction to complete.
        System.out.println("sleping for 2 seconds.")
        Thread.sleep(2000)
        noOfRetries += 1
      }
      else {
        status = true
      }
    }
    return status
  }

  /**
    * Test whether major compaction is not included in minor compaction.
    */
  test("delete merged folder and check segments") {
    // delete merged segments
    sql("clean files for table stopmajor")

    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "stopmajor"
    )
    val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier

    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(
      absoluteTableIdentifier)

    // merged segment should not be there
    val segments = segmentStatusManager.getValidAndInvalidSegments.getValidSegments.asScala.map(_.getSegmentNo).toList
    assert(segments.contains("0.1"))
    assert(!segments.contains("0.2"))
    assert(!segments.contains("0"))
    assert(!segments.contains("1"))
    assert(segments.contains("2"))
    assert(segments.contains("3"))

  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists  stopmajor")
  }

}

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
package org.apache.carbondata.spark.testsuite.datacompaction

import java.io.File

import org.apache.carbondata.core.carbon.path.CarbonStorePath
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.load.LoadMetadataDetails
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.lcm.status
import org.apache.carbondata.lcm.status.SegmentStatusManager
import org.scalatest.BeforeAndAfterAll

import scala.collection.JavaConverters._

/**
  * FT for compaction scenario where major segment should not be included in minor.
  */
class MajorCompactionIgnoreInMinorTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
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


    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    val csvFilePath1 = currentDirectory + "/src/test/resources/compaction/compaction1.csv"

    val csvFilePath2 = currentDirectory + "/src/test/resources/compaction/compaction2.csv"
    val csvFilePath3 = currentDirectory + "/src/test/resources/compaction/compaction3.csv"

    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE ignoremajor OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE ignoremajor  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    // compaction will happen here.
    sql("alter table ignoremajor compact 'major'"
    )
    if (checkCompactionCompletedOrNot("0.1")) {
      sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE ignoremajor OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
      )
      sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE ignoremajor  OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
      )
      sql("alter table ignoremajor compact 'minor'"
      )
      if (checkCompactionCompletedOrNot("2.1")) {
        sql("alter table ignoremajor compact 'minor'"
        )
      }

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

      val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
          AbsoluteTableIdentifier(
            CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
            new CarbonTableIdentifier("default", "ignoremajor", noOfRetries + "")
          )
      )
      val segments = segmentStatusManager.getValidSegments().listOfValidSegments.asScala.toList
      segments.foreach(seg =>
        System.out.println( "valid segment is =" + seg)
      )

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
    sql("clean files for table ignoremajor")

    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
        AbsoluteTableIdentifier(
          CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
          new CarbonTableIdentifier("default", "ignoremajor", "rrr")
        )
    )
    // merged segment should not be there
    val segments = segmentStatusManager.getValidSegments.listOfValidSegments.asScala.toList
    assert(segments.contains("0.1"))
    assert(segments.contains("2.1"))
    assert(!segments.contains("2"))
    assert(!segments.contains("3"))

  }

  /**
    * Delete should not work on compacted segment.
    */
  test("delete compacted segment and check status") {
    try {
      sql("delete segment 2 from table ignoremajor")
      assert(false)
    }
    catch {
      case _ => assert(true)
    }
    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
        AbsoluteTableIdentifier(
          CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
          new CarbonTableIdentifier("default", "ignoremajor", "rrr")
        )
    )
    val carbontablePath = CarbonStorePath
      .getCarbonTablePath(CarbonProperties.getInstance
        .getProperty(CarbonCommonConstants.STORE_LOCATION),
        new CarbonTableIdentifier("default", "ignoremajor", "rrr")
      )
      .getMetadataDirectoryPath
    var segs = segmentStatusManager.readLoadMetadata(carbontablePath)

    // status should remain as compacted.
    assert(segs(3).getLoadStatus.equalsIgnoreCase(CarbonCommonConstants.SEGMENT_COMPACTED))

  }

  /**
    * Delete should not work on compacted segment.
    */
  test("delete compacted segment by date and check status") {
    sql(
      "DELETE SEGMENTS FROM TABLE ignoremajor where STARTTIME before" +
        " '2222-01-01 19:35:01'"
    )
    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
        AbsoluteTableIdentifier(
          CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
          new CarbonTableIdentifier("default", "ignoremajor", "rrr")
        )
    )
    val carbontablePath = CarbonStorePath
      .getCarbonTablePath(CarbonProperties.getInstance
        .getProperty(CarbonCommonConstants.STORE_LOCATION),
        new CarbonTableIdentifier("default", "ignoremajor", "rrr")
      )
      .getMetadataDirectoryPath
    var segs = segmentStatusManager.readLoadMetadata(carbontablePath)

    // status should remain as compacted for segment 2.
    assert(segs(3).getLoadStatus.equalsIgnoreCase(CarbonCommonConstants.SEGMENT_COMPACTED))
    // for segment 0.1 . should get deleted
    assert(segs(2).getLoadStatus.equalsIgnoreCase(CarbonCommonConstants.MARKED_FOR_DELETE))

  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }

}

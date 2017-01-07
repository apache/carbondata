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

import scala.collection.JavaConverters._

import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.carbon.datastore.TableSegmentUniqueIdentifier
import org.apache.carbondata.core.carbon.datastore.block.SegmentTaskIndexWrapper
import org.apache.carbondata.core.carbon.path.CarbonStorePath
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.updatestatus.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hadoop.CacheClient

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


    val csvFilePath1 = s"$resourcesPath/compaction/compaction1.csv"
    val csvFilePath2 = s"$resourcesPath/compaction/compaction2.csv"
    val csvFilePath3 = s"$resourcesPath/compaction/compaction3.csv"

    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE ignoremajor OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE ignoremajor  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    // compaction will happen here.
    sql("alter table ignoremajor compact 'major'"
    )
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
    * Test whether major compaction is not included in minor compaction.
    */
  test("delete merged folder and check segments") {
    // delete merged segments
    sql("clean files for table ignoremajor")
    sql("select * from ignoremajor").show()
    val identifier = new AbsoluteTableIdentifier(
          CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
          new CarbonTableIdentifier(
            CarbonCommonConstants.DATABASE_DEFAULT_NAME, "ignoremajor", "rrr")
        )
    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(identifier)

    // merged segment should not be there
    val segments = segmentStatusManager.getValidAndInvalidSegments.getValidSegments.asScala.toList
    assert(segments.contains("0.1"))
    assert(segments.contains("2.1"))
    assert(!segments.contains("2"))
    assert(!segments.contains("3"))
    val cacheClient = new CacheClient(CarbonProperties.getInstance.
      getProperty(CarbonCommonConstants.STORE_LOCATION));
    val segmentIdentifier = new TableSegmentUniqueIdentifier(identifier, "2")
    val wrapper: SegmentTaskIndexWrapper = cacheClient.getSegmentAccessClient.
      getIfPresent(segmentIdentifier)
    assert(null == wrapper)

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
      case _:Throwable => assert(true)
    }
    val carbontablePath = CarbonStorePath
      .getCarbonTablePath(CarbonProperties.getInstance
        .getProperty(CarbonCommonConstants.STORE_LOCATION),
        new CarbonTableIdentifier(
          CarbonCommonConstants.DATABASE_DEFAULT_NAME, "ignoremajor", "rrr")
      )
      .getMetadataDirectoryPath
    val segs = SegmentStatusManager.readLoadMetadata(carbontablePath)

    // status should remain as compacted.
    assert(segs(3).getLoadStatus.equalsIgnoreCase(CarbonCommonConstants.COMPACTED))

  }

  /**
    * Delete should not work on compacted segment.
    */
  test("delete compacted segment by date and check status") {
    sql(
      "DELETE SEGMENTS FROM TABLE ignoremajor where STARTTIME before" +
        " '2222-01-01 19:35:01'"
    )
    val carbontablePath = CarbonStorePath
      .getCarbonTablePath(CarbonProperties.getInstance
        .getProperty(CarbonCommonConstants.STORE_LOCATION),
        new CarbonTableIdentifier(
          CarbonCommonConstants.DATABASE_DEFAULT_NAME, "ignoremajor", "rrr")
      )
      .getMetadataDirectoryPath
    val segs = SegmentStatusManager.readLoadMetadata(carbontablePath)

    // status should remain as compacted for segment 2.
    assert(segs(3).getLoadStatus.equalsIgnoreCase(CarbonCommonConstants.COMPACTED))
    // for segment 0.1 . should get deleted
    assert(segs(2).getLoadStatus.equalsIgnoreCase(CarbonCommonConstants.MARKED_FOR_DELETE))

  }

  override def afterAll {
    sql("drop table if exists  ignoremajor")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }

}

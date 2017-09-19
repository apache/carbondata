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

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties

/**
 * FT for data compaction Minor threshold verification.
 */
class DataCompactionMinorThresholdTest extends QueryTest with BeforeAndAfterAll {
  val carbonTableIdentifier: CarbonTableIdentifier =
    new CarbonTableIdentifier("default", "minorthreshold".toLowerCase(), "1")

  val identifier = new AbsoluteTableIdentifier(storeLocation, carbonTableIdentifier)

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")
    sql("drop table if exists  minorthreshold")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")
    sql(
      "CREATE TABLE IF NOT EXISTS minorthreshold (country String, ID Int, date " +
      "Timestamp, name " +
      "String, " +
      "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
      ".format'"
    )

    val csvFilePath1 = s"$resourcesPath/compaction/compaction1.csv"
    val csvFilePath2 = s"$resourcesPath/compaction/compaction2.csv"
    val csvFilePath3 = s"$resourcesPath/compaction/compaction3.csv"

    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE minorthreshold " +
        "OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE minorthreshold  " +
        "OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath3 + "' INTO TABLE minorthreshold  " +
        "OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath3 + "' INTO TABLE minorthreshold  " +
        "OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    // compaction should happen here.
    sql("alter table minorthreshold compact 'minor'")
  }

  /**
   * Compaction should be completed correctly for minor compaction.
   */
  test("check if compaction is completed correctly for minor.") {

    sql("clean files for table minorthreshold")

    val segmentStatusManager: SegmentStatusManager =
      new SegmentStatusManager(identifier, hadoopConf)
    val segments = segmentStatusManager.getValidAndInvalidSegments.getValidSegments.asScala.toList

    assert(segments.contains("0.2"))
    assert(!segments.contains("0.1"))
    assert(!segments.contains("0"))
    assert(!segments.contains("1"))
    assert(!segments.contains("2"))
    assert(!segments.contains("3"))
  }

  override def afterAll {
    sql("drop table if exists  minorthreshold")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

}

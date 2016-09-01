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

import org.apache.carbondata.core.carbon.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.lcm.status.SegmentStatusManager
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import scala.collection.JavaConverters._

/**
  * FT for compaction scenario where major segment should not be included in minor.
  */
class CompactionSystemLockFeatureTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance().addProperty("carbon.compaction.level.threshold", "2,2")
    sql("drop table if exists  table1")
    sql("drop table if exists  table2")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")
    sql(
      "CREATE TABLE IF NOT EXISTS table1 (country String, ID Int, date Timestamp, name " +
        "String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
        ".format'"
    )
    sql(
      "CREATE TABLE IF NOT EXISTS table2 (country String, ID Int, date Timestamp, name " +
        "String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
        ".format'"
    )


    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    val csvFilePath1 = currentDirectory + "/src/test/resources/compaction/compaction1.csv"

    val csvFilePath2 = currentDirectory + "/src/test/resources/compaction/compaction2.csv"
    val csvFilePath3 = currentDirectory + "/src/test/resources/compaction/compaction3.csv"

    // load table1
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE table1 OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE table1  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )

    // load table2
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE table2 OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE table2  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )

    // create  a file in table 2 so that it will also be compacted.
    val absoluteTableIdentifier = new
        AbsoluteTableIdentifier(
          CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
          new CarbonTableIdentifier("default", "table2", "rrr")
        )
    val carbonTablePath: CarbonTablePath = CarbonStorePath
      .getCarbonTablePath(absoluteTableIdentifier.getStorePath,
        absoluteTableIdentifier.getCarbonTableIdentifier
      )

    val file = carbonTablePath.getMetadataDirectoryPath + CarbonCommonConstants
      .FILE_SEPARATOR + CarbonCommonConstants.majorCompactionRequiredFile

    FileFactory.createNewFile(file, FileFactory.getFileType(file))

    // compaction will happen here.
    sql("alter table table1 compact 'major'"
    )

  }

  /**
    * Test whether major compaction is done for both.
    */
  test("check for compaction in both tables") {
    // delete merged segments
    sql("clean files for table table1")
    sql("clean files for table table2")

    // check for table 1.
    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
        AbsoluteTableIdentifier(
          CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
          new CarbonTableIdentifier("default", "table1", "rrr")
        )
    )
    // merged segment should not be there
    val segments = segmentStatusManager.getValidSegments.listOfValidSegments.asScala.toList
    assert(segments.contains("0.1"))
    assert(!segments.contains("0"))
    assert(!segments.contains("1"))
    // check for table 2.
    val segmentStatusManager2: SegmentStatusManager = new SegmentStatusManager(new
        AbsoluteTableIdentifier(
          CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
          new CarbonTableIdentifier("default", "table2", "rrr1")
        )
    )
    // merged segment should not be there
    val segments2 = segmentStatusManager2.getValidSegments.listOfValidSegments.asScala.toList
    assert(segments2.contains("0.1"))
    assert(!segments2.contains("0"))
    assert(!segments2.contains("1"))

  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    sql("drop table if exists  table1")
    sql("drop table if exists  table2")
  }

}

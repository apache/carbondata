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

import scala.collection.JavaConverters._

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.lcm.status.SegmentStatusManager

/**
 * FT for data compaction Boundary condition verification.
 */
class DataCompactionBoundaryConditionsTest extends QueryTest with BeforeAndAfterAll {
  val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
    .getCanonicalPath
  val resource = currentDirectory + "/src/test/resources/"

  val storeLocation = new File(this.getClass.getResource("/").getPath + "/../test").getCanonicalPath
  val carbonTableIdentifier: CarbonTableIdentifier =
    new CarbonTableIdentifier("default", "boundarytest".toLowerCase(), "1")

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")
    sql("drop table if exists  boundarytest")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")
    sql(
      "CREATE TABLE IF NOT EXISTS boundarytest (country String, ID Int, date " +
      "Timestamp, name " +
      "String, " +
      "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
      ".format'"
    )

  }

  /**
   * Compaction verificatoin in case of no loads.
   */
  test("check if compaction is completed correctly.") {

    try {
      sql("alter table boundarytest compact 'minor'")
      sql("alter table boundarytest compact 'major'")
    }
    catch {
      case e: Exception =>
        assert(false)
    }
  }

  /**
   * Compaction verificatoin in case of one loads.
   */
  test("check if compaction is completed correctly for one load.") {

    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    var csvFilePath1 = currentDirectory + "/src/test/resources/compaction/compaction1.csv"


    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE boundarytest " +
        "OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("alter table boundarytest compact 'minor'")
    sql("alter table boundarytest compact 'major'")

  }


  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }

}

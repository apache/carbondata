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

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.lcm.status.SegmentStatusManager
import org.scalatest.BeforeAndAfterAll

import scala.collection.JavaConverters._

/**
  * FT for data compaction scenario.
  */
class DataCompactionCardinalityBoundryTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    sql("drop table if exists  cardinalityTest")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")
    sql(
      "CREATE TABLE IF NOT EXISTS cardinalityTest (country String, ID String, date Timestamp, name " +
        "String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
        ".format'"
    )


    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    val csvFilePath1 = currentDirectory + "/src/test/resources/compaction/compaction1.csv"

    // loading the rows greater than 256. so that the column cardinality crosses byte boundary.
    val csvFilePath2 = currentDirectory + "/src/test/resources/compaction/compactioncard2.csv"

    val csvFilePath3 = currentDirectory + "/src/test/resources/compaction/compaction3.csv"


    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE cardinalityTest OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE cardinalityTest  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    System.out
      .println("load merge status is " + CarbonProperties.getInstance()
        .getProperty("carbon.enable.load.merge")
      )
    // compaction will happen here.
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath3 + "' INTO TABLE cardinalityTest  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    // compaction will happen here.
    sql("alter table cardinalityTest compact 'major'"
    )

  }

  test("check if compaction is completed or not and  verify select query.") {
    var status = true
    var noOfRetries = 0
    while (status && noOfRetries < 10) {

      val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
          AbsoluteTableIdentifier(
            CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
            new CarbonTableIdentifier("default", "cardinalityTest", "1")
          )
      )
      val segments = segmentStatusManager.getValidAndInvalidSegments.getValidSegments.asScala.toList

      if (!segments.contains("0.1")) {
        // wait for 2 seconds for compaction to complete.
        Thread.sleep(500)
        noOfRetries += 1
      }
      else {
        status = false
      }
    }
    // now check the answers it should be same.
    checkAnswer(
      sql("select country,count(*) from cardinalityTest group by country"),
      Seq(Row("america",1),
        Row("canada",1),
        Row("chile",1),
        Row("china",2),
        Row("england",1),
        Row("burma",152),
        Row("butan",101),
        Row("mexico",1),
        Row("newzealand",1),
        Row("westindies",1),
        Row("india",1),
        Row("iran",1),
        Row("iraq",1),
        Row("ireland",1)
      )
    )
  }

  override def afterAll {
    /* sql("drop table cardinalityTest") */
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "false")
  }

}

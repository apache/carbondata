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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties

/**
  * FT for data compaction scenario.
  */
class DataCompactionTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sql("drop table if exists  normalcompaction")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")
    sql(
      "CREATE TABLE IF NOT EXISTS normalcompaction (country String, ID Int, date Timestamp, name " +
        "String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
        ".format'"
    )

    val csvFilePath1 = s"$resourcesPath/compaction/compaction1.csv"
    val csvFilePath2 = s"$resourcesPath/compaction/compaction2.csv"
    val csvFilePath3 = s"$resourcesPath/compaction/compaction3.csv"

    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE normalcompaction OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE normalcompaction  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    // compaction will happen here.
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath3 + "' INTO TABLE normalcompaction  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    // compaction will happen here.
    sql("alter table normalcompaction compact 'major'"
    )

  }

  test("check if compaction is completed or not.") {
    var status = true
    var noOfRetries = 0
    while (status && noOfRetries < 10) {

      val identifier = new AbsoluteTableIdentifier(
            CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
            new CarbonTableIdentifier(CarbonCommonConstants.DATABASE_DEFAULT_NAME, "normalcompaction", "1")
          )

      val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(identifier)

      val segments = segmentStatusManager.getValidAndInvalidSegments.getValidSegments.asScala.toList

      if (!segments.contains("0.1")) {
        // wait for 2 seconds for compaction to complete.
        Thread.sleep(2000)
        noOfRetries += 1
      }
      else {
        status = false
      }
    }
  }


  test("select country from normalcompaction") {
    // check answers after compaction.
    checkAnswer(
      sql("select country from normalcompaction"),
      Seq(Row("america"),
        Row("canada"),
        Row("chile"),
        Row("china"),
        Row("england"),
        Row("burma"),
        Row("butan"),
        Row("mexico"),
        Row("newzealand"),
        Row("westindies"),
        Row("china"),
        Row("india"),
        Row("iran"),
        Row("iraq"),
        Row("ireland")
      )
    )
  }

  test("delete merged folder and execute query") {
    // delete merged segments
    sql("clean files for table normalcompaction")

    val identifier = new AbsoluteTableIdentifier(
          CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
          new CarbonTableIdentifier(
            CarbonCommonConstants.DATABASE_DEFAULT_NAME, "normalcompaction", "uniqueid")
        )

    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(identifier)

    // merged segment should not be there
    val segments = segmentStatusManager.getValidAndInvalidSegments.getValidSegments.asScala.toList
    assert(!segments.contains("0"))
    assert(!segments.contains("1"))
    assert(!segments.contains("2"))
    assert(segments.contains("0.1"))

    // now check the answers it should be same.
    checkAnswer(
      sql("select country from normalcompaction"),
      Seq(Row("america"),
        Row("canada"),
        Row("chile"),
        Row("china"),
        Row("england"),
        Row("burma"),
        Row("butan"),
        Row("mexico"),
        Row("newzealand"),
        Row("westindies"),
        Row("china"),
        Row("india"),
        Row("iran"),
        Row("iraq"),
        Row("ireland")
      )
    )
  }


  test("check if compaction with Updates") {

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
    sql("drop table if exists  cardinalityUpdatetest")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")

    sql(
      "CREATE TABLE IF NOT EXISTS cardinalityUpdateTest (FirstName String, LastName String, date Timestamp," +
      "phonetype String, serialname String, ID int, salary Int) STORED BY 'org.apache.carbondata" +
      ".format'"
    )

    val csvFilePath1 = s"$resourcesPath/compaction/compactionIUD1.csv"
    val csvFilePath2 = s"$resourcesPath/compaction/compactionIUD2.csv"
    val csvFilePath3 = s"$resourcesPath/compaction/compactionIUD3.csv"
    val csvFilePath4 = s"$resourcesPath/compaction/compactionIUD4.csv"

    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE cardinalityUpdateTest OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE cardinalityUpdateTest OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath3 + "' INTO TABLE cardinalityUpdateTest OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath4 + "' INTO TABLE cardinalityUpdateTest OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )

    // update the first segment
    sql("update cardinalityUpdateTest set (FirstName) = ('FirstTwentyOne') where ID = 2").show()

    // alter table.
    sql("alter table cardinalityUpdateTest compact 'major'")

    // Verify the new updated value in compacted segment.
    // now check the answers it should be same.
    checkAnswer(
      sql("select FirstName from cardinalityUpdateTest where FirstName = ('FirstTwentyOne')"),
      Seq(Row("FirstTwentyOne")
      )
    )

    checkAnswer(
      sql("select count(*) from cardinalityUpdateTest where FirstName = ('FirstTwentyOne')"),
      Seq(Row(1)
      )
    )

    checkAnswer(
      sql("select count(*) from cardinalityUpdateTest"),
      Seq(Row(20)
      )
    )
  }

  override def afterAll {
    sql("drop table if exists normalcompaction")
    sql("drop table if exists cardinalityUpdatetest")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
  }

}

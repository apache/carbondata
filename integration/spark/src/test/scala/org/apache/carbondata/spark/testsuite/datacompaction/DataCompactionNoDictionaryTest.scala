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
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties

/**
  * FT for data compaction scenario.
  */
class DataCompactionNoDictionaryTest extends QueryTest with BeforeAndAfterAll {

  // return segment details
  def getSegments(databaseName : String, tableName : String, tableId : String): List[String] = {
    val identifier = new AbsoluteTableIdentifier(
          CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
          new CarbonTableIdentifier(databaseName, tableName.toLowerCase , tableId))

    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(identifier)
    segmentStatusManager.getValidAndInvalidSegments.getValidSegments.asScala.toList
  }

  var csvFilePath1 = s"$resourcesPath/compaction/compaction1.csv"
  var csvFilePath2 = s"$resourcesPath/compaction/compaction2.csv"
  var csvFilePath3 = s"$resourcesPath/compaction/compaction3.csv"

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")
    sql("DROP TABLE IF EXISTS nodictionaryCompaction")
    sql(
      "CREATE TABLE nodictionaryCompaction (country String, ID Int, date Timestamp, name " +
        "String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
        ".format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='country')"
    )



    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE nodictionaryCompaction " +
        "OPTIONS('DELIMITER' = ',')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE nodictionaryCompaction " +
        "OPTIONS('DELIMITER' = ',')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath3 + "' INTO TABLE nodictionaryCompaction " +
        "OPTIONS('DELIMITER' = ',')"
    )
    // compaction will happen here.
    sql("alter table nodictionaryCompaction compact 'major'"
    )

    // wait for compaction to finish.
    Thread.sleep(1000)
  }

  // check for 15 seconds if the compacted segments has come or not .
  // if not created after 15 seconds then testcase will fail.

  test("check if compaction is completed or not.") {
    var status = true
    var noOfRetries = 0
    while (status && noOfRetries < 10) {

      val segments: List[String] = getSegments(
        CarbonCommonConstants.DATABASE_DEFAULT_NAME, "nodictionaryCompaction", "uni21")

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

  test("select country from nodictionaryCompaction") {
    // check answers after compaction.
    checkAnswer(
      sql("select country from nodictionaryCompaction"),
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
   sql("clean files for table nodictionaryCompaction")

    // merged segment should not be there
    val segments =
      getSegments(CarbonCommonConstants.DATABASE_DEFAULT_NAME, "nodictionaryCompaction", "uni21")
    assert(!segments.contains("0"))
    assert(!segments.contains("1"))
    assert(!segments.contains("2"))
    assert(segments.contains("0.1"))

    // now check the answers it should be same.
    checkAnswer(
      sql("select country from nodictionaryCompaction"),
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
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE nodictionaryCompaction " +
        "OPTIONS('DELIMITER' = ',')"
    )
    sql("delete from table nodictionaryCompaction where segment.id in (0.1,3)")
    checkAnswer(
      sql("select country from nodictionaryCompaction"),
      Seq()
    )
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS nodictionaryCompaction")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
  }

}

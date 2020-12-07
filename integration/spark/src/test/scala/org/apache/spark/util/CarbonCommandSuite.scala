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

package org.apache.spark.util

import java.io.File
import java.sql.Timestamp
import java.util.Date

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.util.CarbonProperties

class CarbonCommandSuite extends QueryTest with BeforeAndAfterAll {

  protected def createAndLoadInputTable(inputTableName: String, inputPath: String): Unit = {
    sql(
      s"""
         | CREATE TABLE $inputTableName
         | (  shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string,
         |    timestampField string,
         |    decimalField decimal(18,2),
         |    dateField string,
         |    charField char(5)
         | )
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$inputPath'
         | INTO TABLE $inputTableName
       """.stripMargin)
  }

  protected def createAndLoadTestTable(tableName: String, inputTableName: String): Unit = {
    sql(
      s"""
         | CREATE TABLE $tableName(
         |    shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    decimalField decimal(18,2),
         |    dateField date,
         |    charField char(5)
         | )
         | USING carbondata
       """.stripMargin)
    sql(
      s"""
         | INSERT INTO TABLE $tableName
         | SELECT shortField, intField, bigintField, doubleField, stringField,
         | from_unixtime(unix_timestamp(timestampField,'yyyy/M/dd')) timestampField, decimalField,
         | cast(to_date(from_unixtime(unix_timestamp(dateField,'yyyy/M/dd'))) as date), charField
         | FROM $inputTableName
       """.stripMargin)
  }

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FORCE.name())
    dropTable("csv_table")
    dropTable("carbon_table")
    dropTable("carbon_table2")
    createAndLoadInputTable("csv_table", s"$resourcesPath/data_alltypes.csv")
    createAndLoadTestTable("carbon_table", "csv_table")
  }

  override def afterAll(): Unit = {
    dropTable("csv_table")
    dropTable("carbon_table")
  }

  private lazy val location = CarbonProperties.getStorePath()


  test("delete segment by id") {
    DeleteSegmentById.main(Array(s"${location}", "carbon_table", "0"))
    assert(!CarbonStore.isSegmentValid("default", "carbon_table", location, "0"))
  }

  test("delete segment by date") {
    createAndLoadTestTable("carbon_table2", "csv_table")
    val time = new Timestamp(new Date().getTime)
    DeleteSegmentByDate.main(Array(s"${location}", "carbon_table2", time.toString))
    assert(!CarbonStore.isSegmentValid("default", "carbon_table2", location, "0"))
    dropTable("carbon_table2")
  }

  test("clean files") {
    val table = "carbon_table3"
    dropTable(table)
    createAndLoadTestTable(table, "csv_table")
    DeleteSegmentById.main(Array(s"${location}", table, "0"))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    CleanFiles.main(Array(s"${location}", table, "true", "true"))
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", table)
    val tablePath = carbonTable.getAbsoluteTableIdentifier.getTablePath
    val f = new File(s"$tablePath/Fact/Part0")
    assert(f.isDirectory)

    // all segment folders should be deleted after CleanFiles command
    assert(f.list().length == 0)
    dropTable(table)
  }

  test("clean files with force clean option") {
    val table = "carbon_table4"
    dropTable(table)
    createAndLoadTestTable(table, "csv_table")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    CleanFiles.main(Array(s"${location}", table, "true", "false"))
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", table)
    val tablePath = carbonTable.getTablePath
    val f = new File(tablePath)
    assert(f.exists())

    dropTable(table)
  }

}

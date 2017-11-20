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

import org.apache.spark.sql.common.util.Spark2QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}

class CarbonCommandSuite extends Spark2QueryTest with BeforeAndAfterAll {

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
         | USING org.apache.spark.sql.CarbonSource
         | OPTIONS ('tableName' '$tableName')
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
    dropTable("csv_table")
    dropTable("carbon_table")
    createAndLoadInputTable("csv_table", s"$resourcesPath/data_alltypes.csv")
    createAndLoadTestTable("carbon_table", "csv_table")
  }

  override def afterAll(): Unit = {
    dropTable("csv_table")
    dropTable("carbon_table")
  }

  private lazy val location =
    CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION)


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
    createAndLoadTestTable(table, "csv_table")
    DeleteSegmentById.main(Array(s"${location}", table, "0"))
    CleanFiles.main(Array(s"${location}", table))
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default_"+table)
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
    CleanFiles.main(Array(s"${location}", table, "true"))
    val tablePath = s"${location}${File.separator}default${File.separator}$table"
    val f = new File(tablePath)
    assert(!f.exists())

    dropTable(table)
  }

  test("test clean files for all") {
    // Create table with compacted segment
    sql("drop table if exists cleanfilesforalldb.carbon_table5")
    sql("drop table if exists cleanfilesforalldb.carbon_table6")
    sql("drop database if exists cleanfilesforalldb")
    sql("create database cleanfilesforalldb")
    sql("create table cleanfilesforalldb.carbon_table5 (col1 int, col2 string) stored by 'carbondata'")
    sql("insert into table cleanfilesforalldb.carbon_table5 select 1, \"abc\"")
    sql("insert into table cleanfilesforalldb.carbon_table5 select 2, \"def\"")
    sql("insert into table cleanfilesforalldb.carbon_table5 select 3, \"hij\"")
    sql("insert into table cleanfilesforalldb.carbon_table5 select 4, \"klm\"")
    sql("insert into table cleanfilesforalldb.carbon_table5 select 4, \"nop\"")
    sql("alter table cleanfilesforalldb.carbon_table5 compact 'minor'")

    // Create table with MARKED_FOR_DELETED
    sql("create table cleanfilesforalldb.carbon_table6 (col1 int, col2 string) stored by 'carbondata'")
    sql("insert into table cleanfilesforalldb.carbon_table6 select 1, \"abc\"")
    sql("insert into table cleanfilesforalldb.carbon_table6 select 2, \"def\"")
    sql("delete from table cleanfilesforalldb.carbon_table6 where segment.id in (0,1,2)")
    sql("clean files for all")

    val res1 = sql("show segments for table cleanfilesforalldb.carbon_table5")
    val res2 = sql("show segments for table cleanfilesforalldb.carbon_table6")
    checkExistence(res1, false, "Compacted")
    checkExistence(res2, false, "Marked for Delete")
    sql("drop table if exists cleanfilesforalldb.carbon_table5")
    sql("drop table if exists cleanfilesforalldb.carbon_table6")
    sql("drop database cleanfilesforalldb.cleanfilesforalldb")
  }

  test("test if delete segments by id is unsupported for pre-aggregate tables") {
    dropTable("preaggMain")
    dropTable("preaggMain_preagg1")
    sql("create table preaggMain (a string, b string, c string) stored by 'carbondata'")
    sql("create datamap preagg1 on table PreAggMain using 'preaggregate' as select a,sum(b) from PreAggMain group by a")
    intercept[UnsupportedOperationException] {
      sql("delete from table preaggMain where segment.id in (1,2)")
    }.getMessage.contains("Delete segment operation is not supported on tables")
    intercept[UnsupportedOperationException] {
      sql("delete from table preaggMain_preagg1 where segment.id in (1,2)")
    }.getMessage.contains("Delete segment operation is not supported on pre-aggregate tables")
    dropTable("preaggMain")
    dropTable("preagg1")
  }

  protected def dropTable(tableName: String): Unit ={
    sql(s"DROP TABLE IF EXISTS $tableName")
  }
}

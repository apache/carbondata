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

package org.apache.carbondata.spark.testsuite.createTable

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

class TestCreateExternalTable extends QueryTest with BeforeAndAfterAll {

  var originDataPath: String = _

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_MULTI_VERSION_TABLE_STATUS, "false")
    sql("DROP TABLE IF EXISTS origin")
    sql("drop table IF EXISTS rsext")
    sql("drop table IF EXISTS rstest1")
    // create carbon table and insert data
    sql("CREATE TABLE origin(key INT, value STRING) STORED AS carbondata")
    sql("INSERT INTO origin select 100,'spark'")
    sql("INSERT INTO origin select 200,'hive'")
    originDataPath = s"$storeLocation/origin"
  }

  override def afterAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_MULTI_VERSION_TABLE_STATUS,
        CarbonCommonConstants.CARBON_ENABLE_MULTI_VERSION_TABLE_STATUS_DEFAULT)
    sql("DROP TABLE IF EXISTS origin")
    sql("drop table IF EXISTS rsext")
    sql("drop table IF EXISTS rstest1")
  }

  test("create external table with existing files") {
    assert(new File(originDataPath).exists())
    sql("DROP TABLE IF EXISTS source")
    if (System
          .getProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
            CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT).equalsIgnoreCase("true") ||
        CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
            CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT).equalsIgnoreCase("true")) {

      intercept[Exception] {
        // create external table with existing files
        sql(
          s"""
             |CREATE EXTERNAL TABLE source
             |STORED AS carbondata
             |LOCATION '$storeLocation/origin'
       """.stripMargin)
      }
    } else {

      // create external table with existing files
      sql(
        s"""
           |CREATE EXTERNAL TABLE source
           |STORED AS carbondata
           |LOCATION '$storeLocation/origin'
       """.stripMargin)
      verifyResult()
    }
  }

  private def verifyResult(): Unit = {
    checkAnswer(sql("SELECT count(*) from source"), sql("SELECT count(*) from origin"))

    checkExistence(sql("describe formatted source"), true, storeLocation + "/origin")

    val carbonTable = CarbonEnv.getCarbonTable(None, "source")(sqlContext.sparkSession)
    assert(carbonTable.isExternalTable)

    sql("DROP TABLE IF EXISTS source")

    // DROP TABLE should not delete data
    assert(new File(originDataPath).exists())
  }

  ignore("create external table with specified schema") {
    assert(new File(originDataPath).exists())
    sql("DROP TABLE IF EXISTS source")
    val ex = intercept[AnalysisException] {
      sql(
        s"""
           |CREATE EXTERNAL TABLE source (key INT)
           |STORED AS carbondata
           |LOCATION '$storeLocation/origin'
     """.stripMargin)
    }
    assert(ex.message.contains("Schema must not be specified for external table"))

    sql("DROP TABLE IF EXISTS source")

    // DROP TABLE should not delete data
    assert(new File(originDataPath).exists())
  }

  test("create external table with empty folder") {
    val exception = intercept[AnalysisException] {
      sql(
        s"""
           |CREATE EXTERNAL TABLE source
           |STORED AS carbondata
           |LOCATION './nothing'
         """.stripMargin)
    }
    assert(exception.getMessage().contains("Unable to infer the schema"))
  }

  test("create external table with CTAS") {
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          |CREATE EXTERNAL TABLE source
          |STORED AS carbondata
          |LOCATION './nothing'
          |AS
          | SELECT * FROM origin
        """.stripMargin)
    }
    assert(exception.getMessage().contains("Create external table as select"))
  }
  test("create external table with post schema resturcture") {
    sql("create table rstest1 (c1 string,c2 int) STORED AS carbondata")
    sql("Alter table rstest1 drop columns(c2)")
    sql(
      "Alter table rstest1 add columns(c4 string) TBLPROPERTIES( " +
      "'DEFAULT.VALUE.c4'='def')")
    sql(s"""CREATE EXTERNAL TABLE rsext STORED AS carbondata LOCATION '$storeLocation/rstest1'""")
    sql("insert into rsext select 'shahid', 1")
    checkAnswer(sql("select * from rstest1"), sql("select * from rsext"))
  }

  test("create external table and non-external table on partition table with location") {
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sql("drop table if exists origin")
    // create carbon table and insert data
    sql("CREATE TABLE origin(a int, b string) partitioned by (c string) STORED AS carbondata")
    sql("INSERT INTO origin select 100,'spark','test1'")
    sql("INSERT INTO origin select 200,'hive','test2'")

    // test external table with partition by with location
    sql("drop table if exists source")
      sql(
        s"""
           |CREATE EXTERNAL TABLE source(a int, b string) partitioned by (c string)
           |stored as carbondata
           |LOCATION '$storeLocation/origin'
       """.stripMargin)
    verifyResult()

    // test table with partition by with location
    sql("drop table if exists source")
    sql(
      s"""
         |CREATE TABLE source(a int, b string) partitioned by (c string)
         |stored as carbondata
         |LOCATION '$storeLocation/origin'
       """.stripMargin)
    FileUtils.deleteDirectory(new File( s"$storeLocation/origin1"))
    val newStoreLocation = s"$storeLocation/origin1"
    FileUtils.copyDirectory(new File(s"$storeLocation/origin/c=test1"), new File(newStoreLocation))
    verifyResult()

    // test without any schema file in location specified
    sql("drop table if exists source")
    sql(
      s"""
         |CREATE TABLE source(a int, b string) partitioned by (c string)
         |stored as carbondata
         |LOCATION '$newStoreLocation'
       """.stripMargin)
    checkAnswer(sql("select * from source"), Seq(Row(100, "spark", "test1")))
    sql("drop table if exists source")

    // test with empty directory
    FileUtils.deleteDirectory(new File( s"$storeLocation/origin1"))
    sql("drop table if exists source")
    sql(
      s"""
         |CREATE TABLE source(a int, b string) partitioned by (c string)
         |stored as carbondata
         |LOCATION '$newStoreLocation'
       """.stripMargin)
    val tableIdentifier = new TableIdentifier("source", Some("default"))
    val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier)(sqlContext.sparkSession)
    assert(carbonTable.isTransactionalTable && carbonTable.isHivePartitionTable)
    sql("INSERT INTO source select 100,'spark','test1'")
    checkAnswer(sql("select * from source"), Seq(Row(100, "spark", "test1")))
    sql("drop table if exists source")
  }

  test("test create table with location") {
    // test non-partition table
    val newStoreLocation = s"$storeLocation/origin1"
    FileUtils.deleteDirectory(new File(newStoreLocation))
    sql("drop table if exists source")
    sql(
      s"""
         |CREATE TABLE source(a int, b string,c string)
         |stored as carbondata
         |LOCATION '$newStoreLocation'
       """.stripMargin)
    val tableIdentifier = new TableIdentifier("source", Some("default"))
    val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier)(sqlContext.sparkSession)
    assert(carbonTable.isTransactionalTable)
    sql("INSERT INTO source select 100,'spark','test1'")
    checkAnswer(sql("select * from source"), Seq(Row(100, "spark", "test1")))
    sql("drop table if exists source")
  }

  test("test create external table on transactional table location") {
    sql("DROP TABLE IF EXISTS table1")
    sql("DROP TABLE IF EXISTS table2")
    sql("DROP TABLE IF EXISTS table3")
    try {
      sql("create table table1 (roll string) STORED AS carbondata")
      sql("insert into table1 values('abc')")
      val table1 = CarbonEnv.getCarbonTable(Some("default"), "table1")(sqlContext.sparkSession)
      val lastMdtFileTable1 = FileFactory
        .getCarbonFile(FileFactory.getUpdatedFilePath(table1.getTablePath + "/Metadata/schema"))
        .getLastModifiedTime
      sql(
        s"""CREATE EXTERNAL TABLE table2 STORED AS carbondata
           | LOCATION
           |'${ table1.getTablePath }' """.stripMargin)
      val table2 = CarbonEnv.getCarbonTable(Some("default"), "table2")(sqlContext.sparkSession)
      val lastMdtFileTable2 = FileFactory
        .getCarbonFile(FileFactory.getUpdatedFilePath(table2.getTablePath + "/Metadata/schema"))
        .getLastModifiedTime
      assert(lastMdtFileTable1 == lastMdtFileTable2)
      checkAnswer(sql("select * from table1"), sql("select * from table2"))
      // verify insert into table1 and check results of table1 and table2
      sql("insert into table1 values('abcd')")
      checkAnswer(sql("select * from table1"), sql("select * from table2"))
      // verify delete from table1 and check results of table1 and table2
      sql("delete from table1 where roll='abc'")
      checkAnswer(sql("select * from table1"), sql("select * from table2"))
      // verify insert into table2 and check results of table1 and table2
      sql("insert into table2 values('abcde')")
      checkAnswer(sql("select * from table1"), sql("select * from table2"))
      // verify delete from table2 and check results of table1 and table2
      sql("delete from table1 where roll='abcde'")
      val res = sql("select * from table1")
      checkAnswer(sql("select * from table1"), sql("select * from table2"))
      // drop table2 and test result of table1
      sql("DROP TABLE IF EXISTS table2")
      checkAnswer(sql("select count(*) from table1"), Seq(Row(1)))
      checkAnswer(sql("select * from table1"), res)
      sql(
        s"""CREATE EXTERNAL TABLE table3 STORED AS carbondata
           | LOCATION
           |'${ table1.getTablePath }' """.stripMargin)
      checkAnswer(sql("select * from table1"), sql("select * from table3"))
      // drop table1 and test result of table3
      sql("DROP TABLE IF EXISTS table1")
      checkAnswer(sql("select count(*) from table3"), Seq(Row(0)))
    } finally {
      sql("DROP TABLE IF EXISTS table1")
      sql("DROP TABLE IF EXISTS table2")
      sql("DROP TABLE IF EXISTS table3")
    }
  }

  test("test create external table on transactional partition table location") {
    sql("DROP TABLE IF EXISTS table1")
    sql("DROP TABLE IF EXISTS table2")
    sql("DROP TABLE IF EXISTS table3")
    try {
      sql("create table table1 (name string) partitioned by (dept int) STORED AS carbondata")
      sql("insert into table1 values('abc', 1)")
      val table1 = CarbonEnv.getCarbonTable(Some("default"), "table1")(sqlContext.sparkSession)
      val lastMdtFileTable1 = FileFactory
        .getCarbonFile(FileFactory.getUpdatedFilePath(table1.getTablePath + "/Metadata/schema"))
        .getLastModifiedTime
      sql(
        s"""CREATE EXTERNAL TABLE table2(name string) partitioned by (dept int) STORED AS carbondata
           | LOCATION
           |'${ table1.getTablePath }' """.stripMargin)
      val table2 = CarbonEnv.getCarbonTable(Some("default"), "table2")(sqlContext.sparkSession)
      val lastMdtFileTable2 = FileFactory
        .getCarbonFile(FileFactory.getUpdatedFilePath(table2.getTablePath + "/Metadata/schema"))
        .getLastModifiedTime
      assert(lastMdtFileTable1 == lastMdtFileTable2)
      checkAnswer(sql("select * from table1"), sql("select * from table2"))
      // verify insert into table1 and check results of table1 and table2
      sql("insert into table1 values('abcd', 2)")
      checkAnswer(sql("select * from table1"), sql("select * from table2"))
      // verify delete from table1 and check results of table1 and table2
      sql("delete from table1 where name='abc'")
      checkAnswer(sql("select * from table1"), sql("select * from table2"))
      // verify insert into table2 and check results of table1 and table2
      sql("insert into table2 values('abcde', 2)")
      checkAnswer(sql("select * from table1"), sql("select * from table2"))
      // verify delete from table2 and check results of table1 and table2
      sql("delete from table1 where name='abcde'")
      val res = sql("select * from table1")
      checkAnswer(sql("select * from table1"), sql("select * from table2"))
      // drop table2 and test result of table1
      sql("DROP TABLE IF EXISTS table2")
      checkAnswer(sql("select count(*) from table1"), Seq(Row(1)))
      checkAnswer(sql("select * from table1"), res)
      sql(
        s"""CREATE EXTERNAL TABLE table3(name string) partitioned by (dept int) STORED AS carbondata
           | LOCATION
           |'${ table1.getTablePath }' """.stripMargin)
      checkAnswer(sql("select * from table1"), sql("select * from table3"))
      // drop table1 and test result of table3
      sql("DROP TABLE IF EXISTS table1")
      checkAnswer(sql("select count(*) from table3"), Seq(Row(0)))
    } finally {
      sql("DROP TABLE IF EXISTS table1")
      sql("DROP TABLE IF EXISTS table2")
      sql("DROP TABLE IF EXISTS table3")
    }
  }

}

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
package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.sdk.file.CarbonWriter
import org.apache.carbondata.spark.testsuite.secondaryindex.TestSecondaryIndexUtils.isFilterPushedDownToSI

class TestSIWithPartition extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    sql("drop table if exists uniqdata1")
    sql(
      "CREATE TABLE uniqdata1 (CUST_ID INT,CUST_NAME STRING,DOB timestamp,DOJ timestamp," +
      "BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 DECIMAL(30, 10)," +
      "DECIMAL_COLUMN2 DECIMAL(36, 10),Double_COLUMN1 double, Double_COLUMN2 double," +
      "INTEGER_COLUMN1 int) PARTITIONED BY(ACTIVE_EMUI_VERSION string) STORED AS carbondata " +
      "TBLPROPERTIES('TABLE_BLOCKSIZE'='256 MB')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data_2000.csv' INTO " +
        "TABLE uniqdata1 partition(ACTIVE_EMUI_VERSION='abc') OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID," +
        "CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1," +
        "DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data_2000.csv' INTO " +
        "TABLE uniqdata1 partition(ACTIVE_EMUI_VERSION='abc') OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID," +
        "CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1," +
        "DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data_2000.csv' INTO " +
        "TABLE uniqdata1 partition(ACTIVE_EMUI_VERSION='abc') OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID," +
        "CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1," +
        "DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data_2000.csv' INTO " +
        "TABLE uniqdata1 partition(ACTIVE_EMUI_VERSION='abc') OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID," +
        "CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1," +
        "DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
  }

  test("Testing SI on partition column") {
    sql("drop index if exists indextable1 on uniqdata1")
    assert(intercept[UnsupportedOperationException] {
      sql("create index indextable1 on table uniqdata1 (ACTIVE_EMUI_VERSION) AS 'carbondata'")
    }.getMessage.contains("Secondary Index cannot be created on a partition column"))
  }

  test("Testing SI without partition column") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql("select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108')")
        .collect().toSeq

    checkAnswer(sql("select * from uniqdata1 where CUST_NAME='CUST_NAME_00108'"),
      withoutIndex)

    val df = sql("select * from uniqdata1 where CUST_NAME='CUST_NAME_00108'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  test("Testing SI with partition column[where clause]") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql(
        "select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = " +
        "'abc')")
        .collect().toSeq

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  test("Testing SI on partition table with OR condition") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql(
        "select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108' OR ACTIVE_EMUI_VERSION = " +
        "'abc')")
        .collect().toSeq

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' OR ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' OR ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(true)
    } else {
      assert(false)
    }
  }

  test("Testing SI on partition table with combination of OR OR") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql(
        "select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108' OR CUST_ID='9000' OR " +
        "ACTIVE_EMUI_VERSION = " +
        "'abc')")
        .collect().toSeq

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' OR CUST_ID='9000' OR " +
      "ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' OR CUST_ID='9000' OR " +
      "ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(true)
    } else {
      assert(false)
    }
  }

  test("Testing SI on partition table with combination of OR AND") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql(
        "select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108' OR CUST_ID='9000' AND " +
        "ACTIVE_EMUI_VERSION = " +
        "'abc')")
        .collect().toSeq

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' OR CUST_ID='9000' AND " +
      "ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' OR CUST_ID='9000' AND " +
      "ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(true)
    } else {
      assert(false)
    }
  }

  test("Testing SI on partition table with combination of AND OR") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql(
        "select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108' AND CUST_ID='9000' OR " +
        "ACTIVE_EMUI_VERSION = " +
        "'abc')")
        .collect().toSeq

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' AND CUST_ID='9000' OR " +
      "ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' AND CUST_ID='9000' OR " +
      "ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(true)
    } else {
      assert(false)
    }
  }

  test("Testing SI on partition table with combination of AND AND") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql(
        "select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108' AND CUST_ID='9000' AND " +
        "ACTIVE_EMUI_VERSION = " +
        "'abc')")
        .collect().toSeq

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' AND CUST_ID='9000' AND " +
      "ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' AND CUST_ID='9000' AND " +
      "ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  test("Testing SI on partition table with major compaction") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql(
        "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = " +
        "'abc'")
        .collect().toSeq

    sql("alter table uniqdata1 compact 'major'")

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  test("Testing SI on partition table with minor compaction") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")

    val withoutIndex =
      sql(
        "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = " +
        "'abc'")
        .collect().toSeq

    sql("alter table uniqdata1 compact 'minor'")

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  test("Testing SI on partition table with delete") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")

    checkAnswer(sql(
      "select count(*) from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION =" +
      " 'abc'"),
      Seq(Row(4)))

    sql("delete from uniqdata1 where CUST_NAME='CUST_NAME_00108'").collect()

    checkAnswer(sql(
      "select count(*) from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION =" +
      " 'abc'"),
      Seq(Row(0)))

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  test("Testing SI on partition table with update") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")

    checkAnswer(sql(
      "select count(*) from uniqdata1 where CUST_ID='9000' and ACTIVE_EMUI_VERSION = 'abc'"),
      Seq(Row(4)))
    assert(intercept[RuntimeException] {
      sql("update uniqdata1 d set (d.CUST_ID) = ('8000')  where d.CUST_ID = '9000'").collect()
    }.getMessage.contains("Update is not permitted on table that contains secondary index"))
  }

  test("Testing SI on partition table with rename") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")

    val withoutIndex =
      sql(
        "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = " +
        "'abc'")
        .collect().toSeq

    sql("alter table uniqdata1 change CUST_NAME test string")

    checkAnswer(sql(
      "select * from uniqdata1 where test='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where test='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  test("test secondary index with partition table having mutiple partition columns") {
    sql("drop table if exists partition_table")
    sql(s"""
         | CREATE TABLE partition_table (
         | stringField string, intField int, shortField short, stringField1 string)
         | STORED AS carbondata
         | PARTITIONED BY (hour_ string, date_ string, sec_ string)
         | TBLPROPERTIES ('SORT_COLUMNS'='hour_,date_,stringField', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"drop index if exists si_on_multi_part on partition_table")
    sql(s"create index si_on_multi_part on partition_table(stringField1) as 'carbondata'")
    sql("insert into partition_table select 'abc', 1,123,'abc1',2,'mon','ten'")
    checkAnswer(sql(s"select count(*) from si_on_multi_part"), Seq(Row(1)))
    val dataFrame =
      sql(s"select stringField,date_,sec_ from partition_table where stringField1='abc1'")
    checkAnswer(dataFrame, Seq(Row("abc", "mon", "ten")))
    if (!isFilterPushedDownToSI(dataFrame.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    sql("drop table if exists partition_table")
  }

  test("test si with add partition based on location on partition table") {
    sql("drop table if exists partition_table")
    sql("create table partition_table (id int,name String) " +
        "partitioned by(email string) stored as carbondata")
    sql("insert into partition_table select 1,'blue','abc'")
    sql("CREATE INDEX partitionTable_si  on table partition_table (name) as 'carbondata'")
    val schemaFile =
      CarbonTablePath.getSchemaFilePath(
        CarbonEnv.getCarbonTable(None, "partition_table")(sqlContext.sparkSession).getTablePath)
    val sdkWritePath = target + "/" + "def"
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(sdkWritePath))
    val writer = CarbonWriter.builder()
      .outputPath(sdkWritePath)
      .writtenBy("test")
      .withSchemaFile(schemaFile)
      .withCsvInput()
      .build()
    writer.write(Seq("2", "red", "def").toArray)
    writer.write(Seq("3", "black", "def").toArray)
    writer.close()
    sql(s"alter table partition_table add partition (email='def') location '$sdkWritePath'")
    sql("insert into partition_table select 4,'red','def'")
    var extSegmentQuery = sql("select * from partition_table where name = 'red'")
    checkAnswer(extSegmentQuery, Seq(Row(2, "red", "def"), Row(4, "red", "def")))
    val location = target + "/" + "def2"
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(location))
    // add new partition with empty location
    sql(s"alter table partition_table add partition (email='def2') location '$location'")
    sql("insert into partition_table select 3,'red','def2'")
    sql("insert into partition_table select 4,'grey','bcd'")
    sql("insert into partition_table select 5,'red','abc'")
    sql("alter table partition_table compact 'minor'")
    extSegmentQuery = sql("select * from partition_table where name = 'red'")
    checkAnswer(extSegmentQuery, Seq(Row(2, "red", "def"), Row(4, "red", "def"),
      Row(3, "red", "def2"), Row(5, "red", "abc")))
    assert(extSegmentQuery.queryExecution.executedPlan.isInstanceOf[BroadCastSIFilterPushJoin])
    sql("drop table if exists partition_table")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(sdkWritePath))
  }

  test("test si with add multiple partitions based on location on partition table") {
    sql("drop table if exists partition_table")
    sql("create table partition_table (id int,name String) " +
        "partitioned by(email string, age int) stored as carbondata")
    sql("insert into partition_table select 1,'blue','abc', 20")
    sql("CREATE INDEX partitionTable_si  on table partition_table (name) as 'carbondata'")
    val schemaFile =
      CarbonTablePath.getSchemaFilePath(
        CarbonEnv.getCarbonTable(None, "partition_table")(sqlContext.sparkSession).getTablePath)
    val sdkWritePath1 = target + "/" + "def"
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(sdkWritePath1))
    var writer = CarbonWriter.builder()
      .outputPath(sdkWritePath1)
      .writtenBy("test")
      .withSchemaFile(schemaFile)
      .withCsvInput()
      .build()
    writer.write(Seq("2", "red", "def", "25").toArray)
    writer.write(Seq("3", "black", "def", "25").toArray)
    writer.close()
    val sdkWritePath2 = target + "/" + "def2"
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(sdkWritePath2))
    writer = CarbonWriter.builder()
      .outputPath(sdkWritePath2)
      .writtenBy("test")
      .withSchemaFile(schemaFile)
      .withCsvInput()
      .build()
    writer.write(Seq("2", "red", "def2", "22").toArray)
    writer.write(Seq("3", "black", "def2", "22").toArray)
    writer.close()
    sql(
      s"alter table partition_table add partition (email='def', age='25') location " +
      s"'$sdkWritePath1' partition (email='def2', age ='22') location '$sdkWritePath2'")
    sql("insert into partition_table select 4,'red','def',25")
    var extSegmentQuery = sql("select * from partition_table where name = 'red'")
    checkAnswer(extSegmentQuery, Seq(Row(2, "red", "def", 25), Row(4, "red", "def", 25),
      Row(2, "red", "def2", 22)))
    val location1 = target + "/" + "xyz"
    val location2 = target + "/" + "xyz2"
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(location1))
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(location2))
    // add new partitions with empty location
    sql(
      s"alter table partition_table add partition (email='xyz', age='25') location " +
      s"'$location1' partition (email='xyz2', age ='22') location '$location2'")
    sql("insert into partition_table select 2,'red','xyz',25")
    sql("insert into partition_table select 3,'red','xyz2',22")
    sql("insert into partition_table select 4,'grey','bcd',23")
    sql("insert into partition_table select 5,'red','abc',22")
    sql("alter table partition_table compact 'minor'")
    extSegmentQuery = sql("select * from partition_table where name = 'red'")
    checkAnswer(extSegmentQuery, Seq(Row(2, "red", "def", 25), Row(4, "red", "def", 25),
      Row(2, "red", "def2", 22), Row(2, "red", "xyz", 25), Row(3, "red", "xyz2", 22),
      Row(5, "red", "abc", 22)))
    assert(extSegmentQuery.queryExecution.executedPlan.isInstanceOf[BroadCastSIFilterPushJoin])
    sql("drop table if exists partition_table")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(sdkWritePath1))
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(sdkWritePath2))
  }

  test("test add and drop partition on partition table") {
    sql("drop table if exists partition_table")
    sql("create table partition_table (id int,name String) " +
        "partitioned by(email string) stored as carbondata")
    sql("insert into partition_table select 1,'blue','abc'")
    sql("CREATE INDEX partitionTable_si  on table partition_table (name) as 'carbondata'")
    val schemaFile =
      CarbonTablePath.getSchemaFilePath(
        CarbonEnv.getCarbonTable(None, "partition_table")(sqlContext.sparkSession).getTablePath)
    val sdkWritePath = target + "/" + "def"
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(sdkWritePath))
    val writer = CarbonWriter.builder()
      .outputPath(sdkWritePath)
      .writtenBy("test")
      .withSchemaFile(schemaFile)
      .withCsvInput()
      .build()
    writer.write(Seq("2", "red", "def").toArray)
    writer.write(Seq("3", "black", "def").toArray)
    writer.close()
    sql(s"alter table partition_table add partition (email='def') location '$sdkWritePath'")
    sql("insert into partition_table select 4,'red','def'")
    sql("alter table partition_table drop partition (email='def')")
    var extSegmentQuery = sql("select count(*) from partition_table where name = 'red'")
    checkAnswer(extSegmentQuery, Row(0))
    sql("insert into partition_table select 5,'red','def'")
    extSegmentQuery = sql("select count(*) from partition_table where name = 'red'")
    checkAnswer(extSegmentQuery, Row(1))
    sql("drop table if exists partition_table")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(sdkWritePath))
  }

  override protected def afterAll(): Unit = {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("drop table if exists uniqdata1")
    sql("drop table if exists partition_table")
  }
}

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

package org.apache.carbondata.view.rewrite

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.sdk.file.CarbonWriter

/**
 * Test class for MV to verify partition scenarios
 */
class TestPartitionWithMV extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {
  // scalastyle:off lineLength
  val testData = s"$resourcesPath/sample.csv"

  override def beforeAll(): Unit = {
    defaultConfig()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql("drop database if exists partition_mv cascade")
    sql("create database partition_mv")
    sql("use partition_mv")
    sql(
      """
        | CREATE TABLE par(id INT, name STRING, age INT) PARTITIONED BY(city STRING)
        | STORED AS carbondata
      """.stripMargin)
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string) partitioned by (age int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
  }

  override def afterAll(): Unit = {
    sql("drop database if exists partition_mv cascade")
    sql("use default")
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
  }

  override def beforeEach(): Unit = {
    sql("drop table if exists partitionone")
    sql("drop table if exists updatetime_8")
    sql("drop materialized view if exists p1")
    sql("drop materialized view if exists p2")
    sql("drop materialized view if exists p3")
    sql("drop materialized view if exists p4")
    sql("drop materialized view if exists p5")
    sql("drop materialized view if exists p6")
    sql("drop materialized view if exists p7")
    sql("drop materialized view if exists ag1")
    sql("drop materialized view if exists ag2")
    sql("drop materialized view if exists ag3")
    sql("drop materialized view if exists ag4")
    sql("drop materialized view if exists ag5")
  }

  // Create mv table on partition with partition column in aggregation only.
  test("test mv table creation on partition table with partition col as aggregation") {
    sql("create materialized view p1 as select id, sum(city) from par group by id")
    assert(!CarbonEnv.getCarbonTable(Some("partition_mv"), "p1")(sqlContext.sparkSession).isHivePartitionTable)
  }

  // Create mv table on partition with partition column in projection and aggregation only.
  test("test mv table creation on partition table with partition col as projection") {
    sql("create materialized view p2 as select id, city, min(city) from par group by id,city ")
    assert(CarbonEnv.getCarbonTable(Some("partition_mv"), "p2")(sqlContext.sparkSession).isHivePartitionTable)
  }

  // Create mv table on partition with partition column as group by.
  test("test mv table creation on partition table with partition col as group by") {
    sql("create materialized view p3 as select id, city, max(city) from par group by id,city ")
    assert(CarbonEnv.getCarbonTable(Some("partition_mv"), "p3")(sqlContext.sparkSession).isHivePartitionTable)
  }

  // Create mv table on partition without partition column.
  test("test mv table creation on partition table without partition column") {
    sql("create materialized view p4 as select name, count(id) from par group by name ")
    assert(!CarbonEnv.getCarbonTable(Some("partition_mv"), "p4")(sqlContext.sparkSession).isHivePartitionTable)
    sql("drop materialized view if exists p4")
  }

  test("test data correction with insert overwrite") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("create materialized view p1 as select empname, year, sum(year),month,day from partitionone group by empname, year, month,day")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert overwrite table partitionone values('v',2,2014,1,1)")
    checkAnswer(sql("select * from partitionone"), Seq(Row("v", 2, 2014, 1, 1)))
    checkAnswer(sql("select * from p1"), Seq(Row("v", 2014, 2014, 1, 1)))
    checkAnswer(sql("select empname, sum(year) from partitionone group by empname, year, month,day"), Seq(Row("v", 2014)))
    val df1 = sql(s"select empname, sum(year) from partitionone group by empname, year, month,day")
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "p1"))
    assert(CarbonEnv.getCarbonTable(Some("partition_mv"), "p1")(sqlContext.sparkSession).isHivePartitionTable)
  }

  test("test data correction with insert overwrite on different value") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("create materialized view p1 as select empname, year, sum(year),month,day from partitionone group by empname, year, month,day")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert overwrite table partitionone values('v',2,2015,1,1)")
    checkAnswer(sql("select * from partitionone"),
      Seq(Row("k", 2, 2014, 1, 1), Row("v", 2, 2015, 1, 1)))
    val df1 = sql(s"select empname, sum(year) from partitionone group by empname, year, month,day")
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "p1"))
    checkAnswer(sql("select * from p1"),
      Seq(Row("k", 2014, 2014, 1, 1), Row("v", 2015, 2015, 1, 1)))
  }

  test("test to check column ordering in parent and child table") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("create materialized view p1 as select empname,  year, sum(year),month,day from partitionone group by empname, month, year,day")
    val parentTable = CarbonEnv.getCarbonTable(Some("partition_mv"), "partitionone")(sqlContext.sparkSession)
    val childTable = CarbonEnv.getCarbonTable(Some("partition_mv"), "p1")(sqlContext.sparkSession)
    val parentPartitionColumns = parentTable.getPartitionInfo.getColumnSchemaList
    val childPartitionColumns = childTable.getPartitionInfo.getColumnSchemaList
    assert(parentPartitionColumns.asScala.zip(childPartitionColumns.asScala).forall {
      case (a, b) =>
        a.getColumnName
          .equalsIgnoreCase(b.getColumnName
            .substring(b.getColumnName.lastIndexOf("_") + 1, b.getColumnName.length))
    })
  }

  test("test data after minor compaction on partition table with mv") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("create materialized view p1 as select empname,  year, sum(year),month,day from partitionone group by empname, year, month,day")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2014,1,2)")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2015,2,1)")
    sql("alter table partitionone compact 'minor'")
    val showSegments = sql("show segments for table partitionone").collect()
      .map { a => (a.get(0), a.get(1)) }
    assert(showSegments.count(_._2 == "Success") == 1)
    assert(showSegments.count(_._2 == "Compacted") == 4)
    assert(CarbonEnv.getCarbonTable(Some("partition_mv"), "p1")(sqlContext.sparkSession).isHivePartitionTable)
  }

  test("test data after major compaction on partition table with mv") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("create materialized view p1 as select empname,  year, sum(year),month,day from partitionone group by empname, year, month,day")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2014,1,2)")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2015,2,1)")
    sql("insert into partitionone values('k',2,2015,2,1)")
    sql("alter table partitionone compact 'major'")
    val showSegments = sql("show segments for table partitionone").collect()
      .map { a => (a.get(0), a.get(1)) }
    assert(showSegments.count(_._2 == "Success") == 1)
    assert(showSegments.count(_._2 == "Compacted") == 5)
  }

  test("test drop partition 1") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("create materialized view p1 as select empname,  year, sum(year),month,day from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2014,1,2)")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2015,2,1)")
    sql("insert into partitionone values('k',2,2015,2,1)")
    sql("alter table partitionone drop partition(day=1)")
    checkAnswer(sql("select * from partitionone"), Seq(Row("k", 2, 2014, 1, 2)))
    checkAnswer(sql("select * from p1"), Seq(Row("k", 2014, 2014, 1, 2)))
  }

  test("test drop partition 2") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("create materialized view p1 as select empname,  year, sum(year),month,day from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2014,1,2)")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2015,2,3)")
    sql("insert into partitionone values('k',2,2015,2,1)")
    sql("alter table partitionone drop partition(day=1)")
    checkAnswer(sql("select * from partitionone"),
      Seq(Row("k", 2, 2014, 1, 2), Row("k", 2, 2015, 2, 3)))
    checkAnswer(sql("select * from p1"),
      Seq(Row("k", 2014, 2014, 1, 2), Row("k", 2015, 2015, 2, 3)))
  }

  test("test drop partition directory") {
    sql("drop table if exists droppartition")
    sql(
      """
        | CREATE TABLE if not exists droppartition (empname String, age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("create materialized view p1  as select empname,  year, sum(year),month,day from droppartition group by empname, year, month, day")
    sql("insert into droppartition values('k',2,2014,1,1)")
    sql("insert into droppartition values('k',2,2015,2,3)")
    sql("alter table droppartition drop partition(year=2015,month=2,day=3)")
    sql("clean files for table droppartition options('force'='true')")
    val table = CarbonEnv.getCarbonTable(Option("partition_mv"), "droppartition")(sqlContext.sparkSession)
    val mvTable = CarbonEnv.getCarbonTable(Option("partition_mv"), "droppartition")(sqlContext.sparkSession)
    val mvtablePath = mvTable.getTablePath
    val tablePath = table.getTablePath
    val carbonFiles = FileFactory.getCarbonFile(tablePath).listFiles().filter{
      file => file.getName.equalsIgnoreCase("year=2015")
    }
    val mvCarbonFiles = FileFactory.getCarbonFile(mvtablePath).listFiles().filter{
      file => file.getName.equalsIgnoreCase("year=2015")
    }
    assert(mvCarbonFiles.length == 0)
    assert(carbonFiles.length == 0)
  }

  test("test data with filter query") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("create materialized view p1 as select empname,  year, sum(year),month,day from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2014,1,2)")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2015,2,3)")
    sql("insert into partitionone values('k',2,2015,2,1)")
    sql("alter table partitionone drop partition(day=1)")
    checkAnswer(sql(
      "select empname, sum(year) from partitionone where day=3 group by empname, year, month, day"),
      Seq(Row("k", 2015)))
    checkAnswer(sql("select * from p1"),
      Seq(Row("k", 2014, 2014, 1, 2), Row("k", 2015, 2015, 2, 3)))
  }

  test("test drop partition 3") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String,age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("create materialized view p1 as select empname,  year, sum(year),month,day from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2014,1,2)")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2015,2,3)")
    sql("insert into partitionone values('k',2,2015,2,1)")
    sql("alter table partitionone drop partition(day=1,month=1)")
    checkAnswer(sql("select * from partitionone"),
      Seq(Row("k", 2, 2014, 1, 2), Row("k", 2, 2015, 2, 3), Row("k", 2, 2015, 2, 1)))
    checkAnswer(sql("select * from p1"),
      Seq(Row("k", 2014, 2014, 1, 2), Row("k", 2015, 2015, 2, 3), Row("k", 2015, 2015, 2, 1)))
  }

  test("test drop partition 4") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("create materialized view p1 as select empname,  year, sum(year),month,day from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2014,1,2)")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2015,2,3)")
    sql("insert into partitionone values('k',2,2015,2,1)")
    sql("alter table partitionone drop partition(year=2014,day=1)")
    checkAnswer(sql("select * from partitionone"),
      Seq(Row("k", 2, 2014, 1, 2), Row("k", 2, 2015, 2, 3), Row("k", 2, 2015, 2, 1)))
    checkAnswer(sql("select * from p1"),
      Seq(Row("k", 2014, 2014, 1, 2), Row("k", 2015, 2015, 2, 3), Row("k", 2015, 2015, 2, 1)))
  }

  test("test drop partition 5") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("create materialized view p1 as select empname,  year, sum(year),month,day from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2014,1,2)")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2015,2,3)")
    sql("insert into partitionone values('k',2,2015,2,1)")
    sql("alter table partitionone drop partition(year=2014,month=1, day=1)")

    checkAnswer(sql("select * from partitionone"),
      Seq(Row("k", 2, 2014, 1, 2), Row("k", 2, 2015, 2, 3), Row("k", 2, 2015, 2, 1)))
    checkAnswer(sql("select * from p1"),
      Seq(Row("k", 2014, 2014, 1, 2), Row("k", 2015, 2015, 2, 3), Row("k", 2015, 2015, 2, 1)))
  }

  test("test drop partition 6") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("create materialized view p1 as select empname,  year, sum(year),month,day from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2014,1,2)")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2015,2,3)")
    sql("insert into partitionone values('k',2,2015,2,1)")
    sql("alter table partitionone drop partition(year=2014,month=1, day=1)")
    checkAnswer(sql("select * from partitionone"),
      Seq(Row("k", 2, 2014, 1, 2), Row("k", 2, 2015, 2, 3), Row("k", 2, 2015, 2, 1)))
    checkAnswer(sql("select * from p1"),
      Seq(Row("k", 2014, 2014, 1, 2), Row("k", 2015, 2015, 2, 3), Row("k", 2015, 2015, 2, 1)))
  }

  test("test drop partition 7") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String,age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("drop materialized view if exists p2")
    sql(
      "create materialized view p1 as select empname, year,sum(year),day from partitionone group by empname, year, day")
    sql(
      "create materialized view p2   as select empname, month,sum(year) from partitionone group by empname, month")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2014,1,2)")

        val exceptionMessage = intercept[Exception] {
      sql("alter table partitionone drop partition(year=2014,month=1, day=1)")
    }.getMessage
    assert(exceptionMessage.contains("Cannot drop partition as one of the partition"))
    assert(exceptionMessage.contains("p2"))
    assert(exceptionMessage.contains("p1"))
  }

  test("test drop partition 8") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("drop materialized view if exists p2")
    sql("drop materialized view if exists p3")
    sql(
      "create materialized view p1 as select empname, year,month,sum(year) from partitionone group by empname, year, month")
    sql(
      "create materialized view p2   as select empname, month, day, sum(year) from partitionone group by empname, month, day")
    sql(
      "create materialized view p3   as select empname,month,sum(year) from partitionone group by empname, month")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2014,1,2)")
    val exceptionMessage = intercept[Exception] {
      sql("alter table partitionone drop partition(year=2014,month=1, day=1)")
    }.getMessage
    assert(exceptionMessage.contains("Cannot drop partition as one of the partition"))
    assert(!exceptionMessage.contains("p2"))
    assert(exceptionMessage.contains("p3"))
    assert(exceptionMessage.contains("p1"))
  }

  test("test drop partition 9") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql(
      "create materialized view p1 as select empname, sum(year) from partitionone group by empname")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2014,1,2)")
    val exceptionMessage = intercept[Exception] {
      sql("alter table partitionone drop partition(year=2014,month=1, day=1)")
    }.getMessage
    assert(exceptionMessage.contains("Cannot drop partition as one of the partition"))
    assert(exceptionMessage.contains("p1"))
  }

  test("test drop partition 10") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, age int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql("drop materialized view if exists p2")
    sql(
      "create materialized view p1 as select empname, sum(year) from partitionone group by empname")
    sql(
      "create materialized view p2 as select empname, year,sum(year),month,day from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2,2014,1,1)")
    sql("insert into partitionone values('k',2,2014,1,2)")
    val exceptionMessage = intercept[Exception] {
      sql("alter table partitionone drop partition(year=2014,month=1, day=1)")
    }.getMessage
    assert(exceptionMessage.contains("Cannot drop partition as one of the partition"))
    assert(exceptionMessage.contains("p1"))
    sql("drop materialized view p1 ")
    sql("alter table partitionone drop partition(year=2014,month=1, day=1)")
  }

  test("test drop partition 11") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql(
      "create materialized view p1 as select empname, year, sum(year) from partitionone group by empname, year")
    sql("insert into partitionone values('k',2014,1,1)")
    val exceptionMessage = intercept[Exception] {
      sql("alter table p1 drop partition(partitionone_year=1)")
    }.getMessage
    assert(exceptionMessage.contains("Cannot drop partition directly on mv"))
  }

  test("test drop partition 12") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql(
      "create materialized view p1 as select empname, sum(year) from partitionone group by empname")
    sql("insert into partitionone values('k',2014,1,1)")
    val exceptionMessage = intercept[Exception] {
      sql("alter table p1 drop partition(year=2014)")
    }.getMessage
    assert(exceptionMessage.contains("operation failed for partition_mv.p1: Not a partitioned table"))
  }

  test("test add partition on mv table") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql(
      "create materialized view p1 as select empname, sum(year) from partitionone group by empname")
    assert(intercept[Exception] {
      sql("alter table p1 add partition(c=1)")
    }.getMessage.equals("Cannot add partition directly on non partitioned table"))
    sql("drop materialized view if exists p1")
    sql(
      "create materialized view p1 as select empname, year from " +
      "partitionone")
    assert(intercept[Exception] {
      sql("alter table p1 add partition(partitionone_year=1)")
    }.getMessage.equals("Cannot add partition directly on child tables"))
  }

  test("test if alter rename is blocked on partition table with mv") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, id int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists p1")
    sql(
      "create materialized view p1 as select empname, sum(year) from partitionone group by empname")
    intercept[Exception] {
      sql("alter table partitionone rename to p")
    }
  }

  test("test dropping partition which has already been deleted") {
    sql("drop table if exists partitiontable")
    sql("create table partitiontable(id int,name string) partitioned by (email string) " +
        "STORED AS carbondata tblproperties('sort_scope'='global_sort')")
    sql("insert into table partitiontable select 1,'huawei','abc'")
    sql("create materialized view ag1 as select count(email),id" +
        " from partitiontable group by id")
    sql("create materialized view ag2 as select sum(email),name" +
        " from partitiontable group by name")
    sql("create materialized view ag3 as select max(email),name" +
        " from partitiontable group by name")
    sql("create materialized view ag4 as select min(email),name" +
        " from partitiontable group by name")
    sql("create materialized view ag5 as select avg(email),name" +
        " from partitiontable group by name")
    sql("alter table partitiontable add partition (email='def')")
    sql("insert into table partitiontable select 1,'huawei','def'")
    sql("drop materialized view ag1")
    sql("drop materialized view ag2")
    sql("drop materialized view ag3")
    sql("drop materialized view ag4")
    sql("drop materialized view ag5")
    sql("alter table partitiontable drop partition(email='def')")
    intercept[Exception] {
      sql("alter table partitiontable drop partition(email='def')")
    }
    sql("drop table if exists partitiontable")
  }

  test("test mv table creation with count(*) on Partition table") {
    sql("drop table if exists partitiontable")
    sql("create table partitiontable(id int,name string) partitioned by (email string) " +
        "STORED AS carbondata tblproperties('sort_scope'='global_sort')")
    sql("insert into table partitiontable select 1,'huawei','abc'")
    sql("drop materialized view if exists ag1")
    sql("create materialized view ag1 as select count(*),id" +
        " from partitiontable group by id")
    sql("insert into table partitiontable select 1,'huawei','def'")
    assert(sql("show materialized views on table partitiontable").collect().head.get(1).toString.equalsIgnoreCase("ag1"))
    sql("drop materialized view ag1")
  }

  test("test blocking partitioning of mv table") {
    sql("drop table if exists updatetime_8")
    sql("create table updatetime_8" +
        "(countryid smallint,hs_len smallint,minstartdate string,startdate string,newdate string,minnewdate string) partitioned by (imex smallint) STORED AS carbondata tblproperties('sort_scope'='global_sort','sort_columns'='countryid,imex,hs_len,minstartdate,startdate,newdate,minnewdate','table_blocksize'='256')")
    sql("drop materialized view if exists ag")
    sql("create materialized view ag properties('partitioning'='false') as select imex,sum(hs_len) from updatetime_8 group by imex")
    val carbonTable = CarbonEnv.getCarbonTable(Some("partition_mv"), "ag")(sqlContext.sparkSession)
    assert(!carbonTable.isHivePartitionTable)
    sql("drop table if exists updatetime_8")
  }

  test("Test data updation after compaction on Partition with mv tables") {
    // Set carbon.timestamp.format will cause heap overhead in this case.
    CarbonProperties.getInstance().removeProperty("carbon.timestamp.format")
    sql("drop table if exists partitionallcompaction")
    sql(
      "create table partitionallcompaction(empno int,empname String,designation String," +
      "workgroupcategory int,workgroupcategoryname String,deptno int,projectjoindate timestamp," +
      "projectenddate date,attendance int,utilization int,salary int) partitioned by (deptname " +
      "String,doj timestamp,projectcode int) STORED AS carbondata tblproperties" +
      "('sort_scope'='global_sort')")
    sql("drop materialized view if exists sensor_1")
    sql(
      "create materialized view sensor_1 as select " +
      "sum(salary),doj, deptname,projectcode from partitionallcompaction group by doj," +
      "deptname,projectcode")
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE
         |partitionallcompaction OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE
         |partitionallcompaction OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE
         |partitionallcompaction PARTITION(deptname='Learning', doj, projectcode) OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"') """.stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE
         |partitionallcompaction PARTITION(deptname='configManagement', doj, projectcode) OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE
         |partitionallcompaction PARTITION(deptname='network', doj, projectcode) OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE
         |partitionallcompaction PARTITION(deptname='protocol', doj, projectcode) OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE
         |partitionallcompaction PARTITION(deptname='security', doj, projectcode) OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql("ALTER TABLE partitionallcompaction COMPACT 'MINOR'").collect()
    checkAnswer(sql("select count(empno) from partitionallcompaction where empno=14"),
      Seq(Row(5)))
    sql("drop table if exists partitionallcompaction")
  }

  test("Test data updation in Aggregate query after compaction on Partitioned table with mv table") {
    sql("drop table if exists updatetime_8")
    sql("create table updatetime_8" +
        "(countryid smallint,hs_len smallint,minstartdate string,startdate string,newdate string,minnewdate string) partitioned by (imex smallint) STORED AS carbondata tblproperties('sort_scope'='global_sort','sort_columns'='countryid,imex,hs_len,minstartdate,startdate,newdate,minnewdate','table_blocksize'='256')")
    sql("drop materialized view if exists ag")
    sql("create materialized view ag as select sum(hs_len), imex from updatetime_8 group by imex")
    sql("insert into updatetime_8 select 21,20,'fbv','gbv','wvsw','vwr',23")
    sql("insert into updatetime_8 select 21,20,'fbv','gbv','wvsw','vwr',24")
    sql("insert into updatetime_8 select 21,20,'fbv','gbv','wvsw','vwr',23")
    sql("insert into updatetime_8 select 21,21,'fbv','gbv','wvsw','vwr',24")
    sql("insert into updatetime_8 select 21,21,'fbv','gbv','wvsw','vwr',24")
    sql("insert into updatetime_8 select 21,21,'fbv','gbv','wvsw','vwr',24")
    sql("insert into updatetime_8 select 21,21,'fbv','gbv','wvsw','vwr',25")
    sql("insert into updatetime_8 select 21,21,'fbv','gbv','wvsw','vwr',25")
    sql("alter table updatetime_8 compact 'minor'")
    sql("alter table updatetime_8 compact 'minor'")
    val df = sql("select sum(hs_len) from updatetime_8 group by imex")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "ag"))
    checkAnswer(sql("select sum(hs_len) from updatetime_8 group by imex"),
      Seq(Row(40), Row(42), Row(83)))
    sql("drop table updatetime_8").collect()
  }

  test("check partitioning for child tables with various combinations") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, id int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED AS carbondata
      """.stripMargin)
    sql(
      "create materialized view p7 as select empname, year, day, sum(year), sum(day) from partitionone group by empname, year, day")
    sql(
      "create materialized view p1 as select empname, sum(year) from partitionone group by empname")
    sql(
      "create materialized view p2 as select empname, year,sum(year) from partitionone group by empname, year")
    sql(
      "create materialized view p3 as select empname, year, month, sum(year), sum(month) from partitionone group by empname, year, month")
    sql(
      "create materialized view p4 as select empname, year,month,day,sum(year) from partitionone group by empname, year, month, day")
    sql(
      "create materialized view p5 as select empname, month,sum(year) from partitionone group by empname, month")
    sql(
      "create materialized view p6 as select empname, month, day, sum(year), sum(month) from partitionone group by empname, month, day")
    assert(!CarbonEnv
      .getCarbonTable(Some("partition_mv"), "p1")(sqlContext.sparkSession)
      .isHivePartitionTable)
    assert(CarbonEnv.getCarbonTable(Some("partition_mv"), "p2")(sqlContext.sparkSession)
             .getPartitionInfo
             .getColumnSchemaList
             .size() == 1)
    assert(CarbonEnv
             .getCarbonTable(Some("partition_mv"), "p3")(sqlContext.sparkSession)
             .getPartitionInfo
             .getColumnSchemaList
             .size == 2)
    assert(CarbonEnv
             .getCarbonTable(Some("partition_mv"), "p4")(sqlContext.sparkSession)
             .getPartitionInfo
             .getColumnSchemaList
             .size == 3)
    assert(!CarbonEnv
      .getCarbonTable(Some("partition_mv"), "p5")(sqlContext.sparkSession)
      .isHivePartitionTable)
    assert(!CarbonEnv
      .getCarbonTable(Some("partition_mv"), "p6")(sqlContext.sparkSession)
      .isHivePartitionTable)
    assert(!CarbonEnv
      .getCarbonTable(Some("partition_mv"), "p7")(sqlContext.sparkSession)
      .isHivePartitionTable)
    sql("drop table if exists partitionone")
  }

  test("test partition at last column") {
    sql("drop table if exists partitionone")
    sql("create table partitionone(a int,b int) partitioned by (c int) STORED AS carbondata")
    sql("insert into partitionone values(1,2,3)")
    sql("drop materialized view if exists dm1")
    sql("create materialized view dm1   as select c,sum(b) from partitionone group by c")
    checkAnswer(sql("select c,sum(b) from partitionone group by c"), Seq(Row(3, 2)))
    sql("drop table if exists partitionone")
  }

  test("test partition on MV with sort column") {
    sql("drop table if exists partitionone")
    sql("create table if not exists partitionone (ts timestamp, " +
        "metric STRING, tags_id STRING, value DOUBLE) partitioned by (ts1 timestamp,ts2 timestamp) stored as carbondata TBLPROPERTIES ('SORT_COLUMNS'='metric,ts2')")
    sql("insert into partitionone values ('2020-09-25 05:38:00','abc','xyz-e01',392.235,'2020-09-25 05:30:00','2020-09-28 05:38:00')")
    val mvQuery = "select tags_id ,metric ,ts1, ts2, timeseries(ts,'thirty_minute') as ts,sum(value),avg(value)," +
                  "min(value),max(value) from partitionone group by metric, tags_id, timeseries(ts,'thirty_minute') ,ts1, ts2"
    val result = sql(mvQuery)
    sql("drop materialized view if exists dm1")
    sql(s"create materialized view dm1  as $mvQuery")
    val df = sql(mvQuery)
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "dm1"))
    checkAnswer(result, df)
    sql("drop table if exists partitionone")
  }

  test("test partition on timeseries column") {
    sql("drop table if exists partitionone")
    sql("create table partitionone(a int,b int) partitioned by (c timestamp,d timestamp) STORED AS carbondata")
    sql("insert into partitionone values(1,2,'2017-01-01 01:00:00','2018-01-01 01:00:00')")
    sql("drop materialized view if exists dm1")
    sql("describe formatted partitionone").collect()
    sql("create materialized view dm1   as select timeseries(c,'day'),sum(b) from partitionone group by timeseries(c,'day')")
    assert(!CarbonEnv
      .getCarbonTable(Some("partition_mv"), "dm1")(sqlContext.sparkSession)
      .isHivePartitionTable)
    assert(sql("select timeseries(c,'day'),sum(b) from partitionone group by timeseries(c,'day')").collect().length  == 1)
    sql("drop table if exists partitionone")
    sql("create table partitionone(a int,b timestamp) partitioned by (c timestamp) STORED AS carbondata")
    sql("insert into partitionone values(1,'2017-01-01 01:00:00','2018-01-01 01:00:00')")
    sql("drop materialized view if exists dm1")
    sql("create materialized view dm1   as select timeseries(b,'day'),c from partitionone group by timeseries(b,'day'),c")
    assert(CarbonEnv
      .getCarbonTable(Some("partition_mv"), "dm1")(sqlContext.sparkSession)
      .isHivePartitionTable)
    assert(sql("select timeseries(b,'day'),c from partitionone group by timeseries(b,'day'),c").collect().length == 1)
    sql("drop table if exists partitionone")
  }

  test("test mv with add partition based on location on partition table") {
    sql("drop table if exists partition_table")
    sql("create table partition_table (id int,name String) " +
        "partitioned by(email string) stored as carbondata")
    sql("drop materialized view if exists partitiontable_mv")
    sql("CREATE materialized view partitiontable_mv as select name from partition_table")
    sql("insert into partition_table select 1,'blue','abc'")
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
    val extSegmentQuery = sql("select name from partition_table")
    assert(TestUtil.verifyMVHit(extSegmentQuery.queryExecution.optimizedPlan, "partitiontable_mv"))
    checkAnswer(extSegmentQuery, Seq(Row("blue"), Row("red"), Row("black")))
    sql("drop table if exists partition_table")
  }
  // scalastyle:on lineLength
}

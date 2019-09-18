/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain id copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.carbondata.spark.testsuite.standardpartition

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, Row}
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.datastore.impl.FileFactory

class StandardPartitionWithPreaggregateTestCase extends CarbonQueryTest with BeforeAndAfterAll {

  val testData = s"$resourcesPath/sample.csv"

  override def beforeAll(): Unit = {
    sql("drop database if exists partition_preaggregate cascade")
    sql("create database partition_preaggregate")
    sql("use partition_preaggregate")
    sql(
      """
        | CREATE TABLE par(id INT, name STRING, age INT) PARTITIONED BY(city STRING)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string) partitioned by (age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
  }

  override def afterAll(): Unit = {
    sql("drop database if exists partition_preaggregate cascade")
    sql("use default")
  }

  // Create aggregate table on partition with partition column in aggregation only.
  test("test preaggregate table creation on partition table with partition col as aggregation") {
    sql("create datamap p1 on table par using 'preaggregate' as select id, sum(city) from par group by id")
    assert(!CarbonEnv.getCarbonTable(Some("partition_preaggregate"), "par_p1")(sqlContext.sparkSession).isHivePartitionTable)
  }

  // Create aggregate table on partition with partition column in projection and aggregation only.
  test("test preaggregate table creation on partition table with partition col as projection") {
    sql("create datamap p2 on table par using 'preaggregate' as select id, city, min(city) from par group by id,city ")
    assert(CarbonEnv.getCarbonTable(Some("partition_preaggregate"), "par_p2")(sqlContext.sparkSession).isHivePartitionTable)
  }

  // Create aggregate table on partition with partition column as group by.
  test("test preaggregate table creation on partition table with partition col as group by") {
    sql("create datamap p3 on table par using 'preaggregate' as select id, max(city) from par group by id,city ")
    assert(CarbonEnv.getCarbonTable(Some("partition_preaggregate"), "par_p3")(sqlContext.sparkSession).isHivePartitionTable)
  }

  // Create aggregate table on partition without partition column.
  test("test preaggregate table creation on partition table without partition column") {
    sql("create datamap p4 on table par using 'preaggregate' as select name, count(id) from par group by name ")
    assert(!CarbonEnv.getCarbonTable(Some("partition_preaggregate"), "par_p4")(sqlContext.sparkSession).isHivePartitionTable)
  }
  
  test("test data correction in aggregate table when partition column is used") {
    sql("create datamap p1 on table maintable using 'preaggregate' as select id, sum(age) from maintable group by id, age")
    checkAnswer(sql("select * from maintable_p1"),
      Seq(Row(1,31,31),
        Row(2,27,27),
        Row(3,70,35),
        Row(4,26,26),
        Row(4,29,29)))
    preAggTableValidator(sql("select id, sum(age) from maintable group by id, age").queryExecution.analyzed, "maintable_p1")
    sql("drop datamap p1 on table maintable")
  }

  test("test data correction in aggregate table when partition column is not used") {
    sql("create datamap p2 on table maintable using 'preaggregate' as select id, max(age) from maintable group by id")
    checkAnswer(sql("select * from maintable_p2"),
      Seq(Row(1,31),
        Row(2,27),
        Row(3,35),
        Row(4,29)))
    preAggTableValidator(sql("select id, max(age) from maintable group by id").queryExecution.analyzed, "maintable_p2")
    sql("drop datamap p2 on table maintable")
  }

  test("test data correction with insert overwrite") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month,day")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert overwrite table partitionone values('v',2014,1,1)")
    checkAnswer(sql("select * from partitionone"), Seq(Row("v",2014,1,1)))
    checkAnswer(sql("select * from partitionone_p1"), Seq(Row("v",2014,2014,1,1)))
  }

  test("test data correction with insert overwrite on different value") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month,day")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert overwrite table partitionone values('v',2015,1,1)")
    checkAnswer(sql("select * from partitionone"), Seq(Row("k",2014,1,1), Row("v",2015,1,1)))
    checkAnswer(sql("select * from partitionone_p1"), Seq(Row("k",2014,2014,1,1), Row("v",2015,2015,1,1)))
  }

  test("test to check column ordering in parent and child table") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, month, year,day")
    val parentTable = CarbonEnv.getCarbonTable(Some("partition_preaggregate"), "partitionone")(sqlContext.sparkSession)
    val childTable = CarbonEnv.getCarbonTable(Some("partition_preaggregate"), "partitionone_p1")(sqlContext.sparkSession)
    val parentPartitionColumns = parentTable.getPartitionInfo.getColumnSchemaList
    val childPartitionColumns = childTable.getPartitionInfo.getColumnSchemaList
    assert(parentPartitionColumns.asScala.zip(childPartitionColumns.asScala).forall {
      case (a,b) =>
        a.getColumnName.equalsIgnoreCase(b.getParentColumnTableRelations.get(0).getColumnName)
    })
  }
  
  test("test data after minor compaction on partition table with pre-aggregate") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month,day")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2014,1,2)")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2015,2,1)")
    sql("alter table partitionone compact 'minor'")
    val showSegments = sql("show segments for table partitionone").collect().map{a=> (a.get(0), a.get(1))}
    assert(showSegments.count(_._2 == "Success") == 1)
    assert(showSegments.count(_._2 == "Compacted") == 4)
  }

  test("test data after major compaction on partition table with pre-aggregate") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month,day")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2014,1,2)")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2015,2,1)")
    sql("insert into partitionone values('k',2015,2,1)")
    sql("alter table partitionone compact 'major'")
    val showSegments = sql("show segments for table partitionone").collect().map{a=> (a.get(0), a.get(1))}
    assert(showSegments.count(_._2 == "Success") == 1)
    assert(showSegments.count(_._2 == "Compacted") == 5)
  }

  test("test drop partition 1") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2014,1,2)")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2015,2,1)")
    sql("insert into partitionone values('k',2015,2,1)")
    sql("alter table partitionone drop partition(day=1)")
    checkAnswer(sql("select * from partitionone"), Seq(Row("k",2014,1,2)))
    checkAnswer(sql("select * from partitionone_p1"), Seq(Row("k",2014,2014,1,2)))
  }

  test("test drop partition 2") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2014,1,2)")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2015,2,3)")
    sql("insert into partitionone values('k',2015,2,1)")
    sql("alter table partitionone drop partition(day=1)")
    checkAnswer(sql("select * from partitionone"), Seq(Row("k",2014,1,2), Row("k",2015,2,3)))
    checkAnswer(sql("select * from partitionone_p1"), Seq(Row("k",2014,2014,1,2), Row("k",2015,2015,2,3)))
  }

  test("test drop partition directory") {
    sql("drop table if exists droppartition")
    sql(
      """
        | CREATE TABLE if not exists droppartition (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("insert into droppartition values('k',2014,1,1)")
    sql("insert into droppartition values('k',2015,2,3)")
    sql("alter table droppartition drop partition(year=2015,month=2,day=3)")
    sql("clean files for table droppartition")
    val table = CarbonEnv.getCarbonTable(Option("partition_preaggregate"), "droppartition")(sqlContext.sparkSession)
    val tablePath = table.getTablePath
    val carbonFiles = FileFactory.getCarbonFile(tablePath).listFiles().filter{
      file => file.getName.equalsIgnoreCase("year=2015")
    }
    assert(carbonFiles.length == 0)
  }

  test("test data with filter query") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2014,1,2)")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2015,2,3)")
    sql("insert into partitionone values('k',2015,2,1)")
    sql("alter table partitionone drop partition(day=1)")
    checkAnswer(sql("select empname, sum(year) from partitionone where day=3 group by empname, year, month, day"), Seq(Row("k",2015)))
    checkAnswer(sql("select * from partitionone_p1"), Seq(Row("k",2014,2014,1,2), Row("k",2015,2015,2,3)))
  }

  test("test drop partition 3") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2014,1,2)")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2015,2,3)")
    sql("insert into partitionone values('k',2015,2,1)")
    sql("alter table partitionone drop partition(day=1,month=1)")
    checkAnswer(sql("select * from partitionone"), Seq(Row("k",2014,1,2), Row("k",2015, 2,3), Row("k",2015, 2,1)))
    checkAnswer(sql("select * from partitionone_p1"), Seq(Row("k",2014,2014,1,2), Row("k",2015,2015,2,3), Row("k",2015,2015,2,1)))
  }

  test("test drop partition 4") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2014,1,2)")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2015,2,3)")
    sql("insert into partitionone values('k',2015,2,1)")
    sql("alter table partitionone drop partition(year=2014,day=1)")
    checkAnswer(sql("select * from partitionone"), Seq(Row("k",2014,1,2), Row("k",2015, 2,3), Row("k",2015, 2,1)))
    checkAnswer(sql("select * from partitionone_p1"), Seq(Row("k",2014,2014,1,2), Row("k",2015,2015, 2,3), Row("k",2015,2015, 2,1)))
  }

  test("test drop partition 5") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2014,1,2)")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2015,2,3)")
    sql("insert into partitionone values('k',2015,2,1)")
    sql("alter table partitionone drop partition(year=2014,month=1, day=1)")

    checkAnswer(sql("select * from partitionone"), Seq(Row("k",2014,1,2), Row("k",2015, 2,3), Row("k",2015, 2,1)))
    checkAnswer(sql("select * from partitionone_p1"), Seq(Row("k",2014,2014,1,2), Row("k",2015,2015,2,3), Row("k",2015,2015,2,1)))
    sql("show partitions partitionone_p1").show()
  }

  test("test drop partition 6") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2014,1,2)")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2015,2,3)")
    sql("insert into partitionone values('k',2015,2,1)")
    sql("alter table partitionone drop partition(year=2014,month=1, day=1)")
    checkAnswer(sql("select * from partitionone"), Seq(Row("k",2014,1,2), Row("k",2015, 2,3), Row("k",2015, 2,1)))
    checkAnswer(sql("select * from partitionone_p1"), Seq(Row("k",2014,2014,1,2), Row("k",2015,2015,2,3), Row("k",2015,2015,2,1)))
  }

  test("test drop partition 7") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      "create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, day")
    sql(
      "create datamap p2 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, month")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2014,1,2)")
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
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      "create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month, day")
    sql(
      "create datamap p2 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, month, day")
    sql(
      "create datamap p3 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2014,1,2)")
    val exceptionMessage = intercept[Exception] {
      sql("alter table partitionone drop partition(year=2014,month=1, day=1)")
    }.getMessage
    assert(exceptionMessage.contains("Cannot drop partition as one of the partition"))
    assert(exceptionMessage.contains("p2"))
    assert(exceptionMessage.contains("p3"))
    assert(!exceptionMessage.contains("p1"))
  }

  test("test drop partition 9") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      "create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname")
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
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      "create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname")
    sql(
      "create datamap p2 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month, day")
    sql("insert into partitionone values('k',2014,1,1)")
    sql("insert into partitionone values('k',2014,1,2)")
    val exceptionMessage = intercept[Exception] {
      sql("alter table partitionone drop partition(year=2014,month=1, day=1)")
    }.getMessage
    assert(exceptionMessage.contains("Cannot drop partition as one of the partition"))
    assert(exceptionMessage.contains("p1"))
    sql("drop datamap p1 on table partitionone")
    sql("alter table partitionone drop partition(year=2014,month=1, day=1)")
  }

  test("test drop partition 11") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      "create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year")
    sql("insert into partitionone values('k',2014,1,1)")
    val exceptionMessage = intercept[Exception] {
      sql("alter table partitionone_p1 drop partition(partitionone_year=1)")
    }.getMessage
    assert(exceptionMessage.contains("Cannot drop partition directly on aggregate table"))
  }

  test("test drop partition 12") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      "create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname")
    sql("insert into partitionone values('k',2014,1,1)")
    val exceptionMessage = intercept[Exception] {
      sql("alter table partitionone_p1 drop partition(year=2014)")
    }.getMessage
    assert(exceptionMessage.contains("operation failed for partition_preaggregate.partitionone_p1: Not a partitioned table"))
  }

  test("test add partition on aggregate table") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      "create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname")
    assert(intercept[Exception] {
      sql("alter table partitionone_p1 add partition(c=1)")
    }.getMessage.equals("Cannot add partition directly on non partitioned table"))
  }

  test("test if alter rename is blocked on partition table with preaggregate") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, id int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      "create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname")
    intercept[Exception] {
      sql("alter table partitionone rename to p")
    }
  }

  test("test dropping partition which has already been deleted") {
    sql("drop table if exists partitiontable")
    sql("create table partitiontable(id int,name string) partitioned by (email string) " +
      "stored by 'carbondata' tblproperties('sort_scope'='global_sort')")
    sql("insert into table partitiontable select 1,'huawei','abc'")
    sql("create datamap ag1 on table partitiontable using 'preaggregate' as select count(email),id" +
      " from partitiontable group by id")
    sql("create datamap ag2 on table partitiontable using 'preaggregate' as select sum(email),name" +
      " from partitiontable group by name")
    sql("create datamap ag3 on table partitiontable using 'preaggregate' as select max(email),name" +
      " from partitiontable group by name")
    sql("create datamap ag4 on table partitiontable using 'preaggregate' as select min(email),name" +
      " from partitiontable group by name")
    sql("create datamap ag5 on table partitiontable using 'preaggregate' as select avg(email),name" +
      " from partitiontable group by name")
    sql("alter table partitiontable add partition (email='def')")
    sql("insert into table partitiontable select 1,'huawei','def'")
    sql("drop datamap ag1 on table partitiontable")
    sql("drop datamap ag2 on table partitiontable")
    sql("drop datamap ag3 on table partitiontable")
    sql("drop datamap ag4 on table partitiontable")
    sql("drop datamap ag5 on table partitiontable")
    sql("alter table partitiontable drop partition(email='def')")
    assert(intercept[Exception] {
      sql("alter table partitiontable drop partition(email='def')")
    }.getMessage.contains("No partition is dropped. One partition spec 'Map(email -> def)' does not exist in table 'partitiontable' database 'partition_preaggregate'"))
  }

  test("test Pre-Aggregate table creation with count(*) on Partition table") {
    sql("drop table if exists partitiontable")
    sql("create table partitiontable(id int,name string) partitioned by (email string) " +
      "stored by 'carbondata' tblproperties('sort_scope'='global_sort')")
    sql("insert into table partitiontable select 1,'huawei','abc'")
    sql("create datamap ag1 on table partitiontable using 'preaggregate' as select count(*),id" +
      " from partitiontable group by id")
    sql("insert into table partitiontable select 1,'huawei','def'")
    assert(sql("show datamap on table partitiontable").collect().head.get(0).toString.equalsIgnoreCase("ag1"))
    sql("drop datamap ag1 on table partitiontable")
  }
  
  test("test blocking partitioning of Pre-Aggregate table") {
    sql("drop table if exists updatetime_8")
    sql("create table updatetime_8" +
      "(countryid smallint,hs_len smallint,minstartdate string,startdate string,newdate string,minnewdate string) partitioned by (imex smallint) stored by 'carbondata' tblproperties('sort_scope'='global_sort','sort_columns'='countryid,imex,hs_len,minstartdate,startdate,newdate,minnewdate','table_blocksize'='256')")
    sql("create datamap ag on table updatetime_8 using 'preaggregate' dmproperties('partitioning'='false') as select imex,sum(hs_len) from updatetime_8 group by imex")
    val carbonTable = CarbonEnv.getCarbonTable(Some("partition_preaggregate"), "updatetime_8_ag")(sqlContext.sparkSession)
    assert(!carbonTable.isHivePartitionTable)
    sql("drop table if exists updatetime_8")
  }

  test("Test data updation after compaction on Partition with Pre-Aggregate tables") {
    sql("drop table if exists partitionallcompaction")
    sql(
      "create table partitionallcompaction(empno int,empname String,designation String," +
      "workgroupcategory int,workgroupcategoryname String,deptno int,projectjoindate timestamp," +
      "projectenddate date,attendance int,utilization int,salary int) partitioned by (deptname " +
      "String,doj timestamp,projectcode int) stored  by 'carbondata' tblproperties" +
      "('sort_scope'='global_sort')")
    sql(
      "create datamap sensor_1 on table partitionallcompaction using 'preaggregate' as select " +
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
  }

  test("Test data updation in Aggregate query after compaction on Partitioned table with Pre-Aggregate table") {
    sql("drop table if exists updatetime_8")
    sql("create table updatetime_8" +
      "(countryid smallint,hs_len smallint,minstartdate string,startdate string,newdate string,minnewdate string) partitioned by (imex smallint) stored by 'carbondata' tblproperties('sort_scope'='global_sort','sort_columns'='countryid,imex,hs_len,minstartdate,startdate,newdate,minnewdate','table_blocksize'='256')")
    sql("create datamap ag on table updatetime_8 using 'preaggregate' as select sum(hs_len) from updatetime_8 group by imex")
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
    checkAnswer(sql("select sum(hs_len) from updatetime_8 group by imex"),Seq(Row(40),Row(42),Row(83)))
  }

  test("check partitioning for child tables with various combinations") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE if not exists partitionone (empname String, id int)
        | PARTITIONED BY (year int, month int,day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      "create datamap p7 on table partitionone using 'preaggregate' as select empname, sum(year), sum(day) from partitionone group by empname, year, day")
    sql(
      "create datamap p1 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname")
    sql(
      "create datamap p2 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year")
    sql(
      "create datamap p3 on table partitionone using 'preaggregate' as select empname, sum(year), sum(month) from partitionone group by empname, year, month")
    sql(
      "create datamap p4 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, year, month, day")
    sql(
      "create datamap p5 on table partitionone using 'preaggregate' as select empname, sum(year) from partitionone group by empname, month")
    sql(
      "create datamap p6 on table partitionone using 'preaggregate' as select empname, sum(year), sum(month) from partitionone group by empname, month, day")
    assert(!CarbonEnv.getCarbonTable(Some("partition_preaggregate"),"partitionone_p1")(sqlContext.sparkSession).isHivePartitionTable)
    assert(CarbonEnv.getCarbonTable(Some("partition_preaggregate"),"partitionone_p2")(sqlContext.sparkSession).getPartitionInfo.getColumnSchemaList.size() == 1)
    assert(CarbonEnv.getCarbonTable(Some("partition_preaggregate"),"partitionone_p3")(sqlContext.sparkSession).getPartitionInfo.getColumnSchemaList.size == 2)
    assert(CarbonEnv.getCarbonTable(Some("partition_preaggregate"),"partitionone_p4")(sqlContext.sparkSession).getPartitionInfo.getColumnSchemaList.size == 3)
    assert(!CarbonEnv.getCarbonTable(Some("partition_preaggregate"),"partitionone_p5")(sqlContext.sparkSession).isHivePartitionTable)
    assert(!CarbonEnv.getCarbonTable(Some("partition_preaggregate"),"partitionone_p6")(sqlContext.sparkSession).isHivePartitionTable)
    assert(!CarbonEnv.getCarbonTable(Some("partition_preaggregate"),"partitionone_p7")(sqlContext.sparkSession).isHivePartitionTable)
  }

  test("test partition at last column") {
    sql("drop table if exists partitionone")
    sql("create table partitionone(a int,b int) partitioned by (c int) stored by 'carbondata'")
    sql("insert into partitionone values(1,2,3)")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1 on table partitionone using 'preaggregate' as select c,sum(b) from partitionone group by c")
    checkAnswer(sql("select c,sum(b) from partitionone group by c"), Seq(Row(3,2)))
    sql("drop table if exists partitionone")
  }

  def preAggTableValidator(plan: LogicalPlan, actualTableName: String) : Unit = {
    var isValidPlan = false
    plan.transform {
      // first check if any preaTable1 scala function is applied it is present is in plan
      // then call is from create preaTable1regate table class so no need to transform the query plan
      case ca:CarbonRelation =>
        if (ca.isInstanceOf[CarbonDatasourceHadoopRelation]) {
          val relation = ca.asInstanceOf[CarbonDatasourceHadoopRelation]
          if(relation.carbonTable.getTableName.equalsIgnoreCase(actualTableName)) {
            isValidPlan = true
          }
        }
        ca
      case logicalRelation:LogicalRelation =>
        if(logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) {
          val relation = logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
          if(relation.carbonTable.getTableName.equalsIgnoreCase(actualTableName)) {
            isValidPlan = true
          }
        }
        logicalRelation
    }
    assert(isValidPlan)
  }

}

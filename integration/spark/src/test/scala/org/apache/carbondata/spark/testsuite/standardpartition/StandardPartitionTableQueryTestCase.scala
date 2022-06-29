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

package org.apache.carbondata.spark.testsuite.standardpartition

import org.apache.spark.sql.{CarbonEnv, DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.sdk.file.CarbonWriter
import org.apache.carbondata.spark.rdd.CarbonScanRDD

class StandardPartitionTableQueryTestCase extends QueryTest with BeforeAndAfterAll {
  // scalastyle:off lineLength
  override def beforeAll {
    dropTable

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd-MM-yyyy")
    sql(
      """
        | CREATE TABLE originTable (empno int, empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE originTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

  }

  test("querying on partition table for int partition column") {
    sql(
      """
        | CREATE TABLE partitionone (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionone OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    val frame = sql(
      "select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno," +
      " deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary " +
      "from partitionone where empno=11 order by empno")
    verifyPartitionInfo(frame, Seq("empno=11"))

    checkAnswer(frame,
      sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable where empno=11 order by empno"))

  }

  test("test load on partition table with load header option set to true") {
    sql("drop table if exists partitionone")
    sql(
      """
        | CREATE TABLE partitionone (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionone OPTIONS('DELIMITER'= ',','HEADER'='true','QUOTECHAR'= '"')""")

    val frame = sql(
      "select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno," +
      " deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary " +
      "from partitionone where empno=11 order by empno")
    verifyPartitionInfo(frame, Seq("empno=11"))

    checkAnswer(frame,
      sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable where empno=11 order by empno"))
    sql("drop table if exists partitionone")
  }

  test("create partition table by dataframe") {
    sql("select * from originTable")
      .write
      .format("carbondata")
      .option("tableName", "partitionxxx")
      .option("partitionColumns", "empno")
      .save("Overwrite")

    val frame = sql(
      "select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno," +
      " deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary " +
      "from partitionxxx where empno=11 order by empno")
    verifyPartitionInfo(frame, Seq("empno=11"))

    checkAnswer(frame,
      sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, " +
          "deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary " +
          "from originTable where empno=11 order by empno"))
    sql("drop table if exists partitionxxx")
  }

  test("querying on partition table for string partition column") {
    sql(
      """
        | CREATE TABLE partitiontwo (empno int, empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (deptname String)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiontwo OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    val frame = sql(
      "select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno," +
      " deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary " +
      "from partitiontwo where deptname='network' and projectcode=928478")
    verifyPartitionInfo(frame, Seq("deptname=network"))

    val frame1 = sql(
      "select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno," +
      " deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary " +
      "from partitiontwo where projectcode=928478")
    checkAnswer(frame1,
      sql( "select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno," +
           " deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary " +
           "from originTable where projectcode=928478"))
    verifyPartitionInfo(frame1,
      Seq("deptname=network",
        "deptname=security",
        "deptname=protocol",
        "deptname=Learning",
        "deptname=configManagement"))

    val frame2 = sql("select distinct deptname from partitiontwo")

    verifyPartitionInfo(frame2,
      Seq("deptname=network",
        "deptname=security",
        "deptname=protocol",
        "deptname=Learning",
        "deptname=configManagement"))

    checkAnswer(frame,
      sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable where where deptname='network' and projectcode=928478 order by empno"))

  }

  test("querying on partition table for more partition columns") {
    sql(
      """
        | CREATE TABLE partitionmany (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (deptname String,doj Timestamp,projectcode int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionmany OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    val frame = sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from partitionmany where doj='2007-01-17 00:00:00'")
    verifyPartitionInfo(frame,
      Seq("deptname=network", "doj=2007-01-17 00:00:00", "projectcode=928478"))
    checkAnswer(frame,
      sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable where doj='2007-01-17 00:00:00'"))

  }

  test("querying on partition table for date partition column") {
    sql(
      """
        | CREATE TABLE partitiondate (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp,attendance int,
        |  deptname String,doj Timestamp,projectcode int,
        |  utilization int,salary int)
        | PARTITIONED BY (projectenddate Date)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiondate OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    val frame = sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from partitiondate where projectenddate = cast('2016-11-30' as date)")
    verifyPartitionInfo(frame, Seq("projectenddate=2016-11-30"))
    checkAnswer(frame,
      sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable where projectenddate = cast('2016-11-30' as date)"))

  }

  test("querying on partition table for date partition column on insert query") {
    sql(
      """
        | CREATE TABLE partitiondateinsert (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp,attendance int,
        |  deptname String,projectcode int,
        |  utilization int,salary int)
        | PARTITIONED BY (projectenddate Date,doj Timestamp)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""insert into partitiondateinsert select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable""")
    val frame = sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from partitiondateinsert where projectenddate = cast('2016-11-30' as date)")
    verifyPartitionInfo(frame, Seq("projectenddate=2016-11-30", "doj=2015-12-01 00:00:00"))
    checkAnswer(frame,
      sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable where projectenddate = cast('2016-11-30' as date)"))

    val frame1 = sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from partitiondateinsert where doj>'2006-01-17 00:00:00'")
    verifyPartitionInfo(frame1,
      Seq("projectenddate=2016-06-29",
        "doj=2010-12-29 00:00:00",
        "doj=2015-12-01 00:00:00",
        "projectenddate=2016-11-12",
        "projectenddate=2016-12-29",
        "doj=2011-11-09 00:00:00",
        "doj=2009-07-07 00:00:00",
        "projectenddate=2016-05-29",
        "doj=2012-10-14 00:00:00",
        "projectenddate=2016-11-30",
        "projectenddate=2016-11-15",
        "doj=2015-05-12 00:00:00",
        "doj=2013-09-22 00:00:00",
        "doj=2008-05-29 00:00:00",
        "doj=2014-08-15 00:00:00",
        "projectenddate=2016-12-30"))
    checkAnswer(frame1,
      sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable where doj>cast('2006-01-17 00:00:00' as Timestamp)"))

  }

  test("badrecords on partition column") {
    sql("create table badrecordsPartition(intField1 int, stringField1 string) partitioned by (intField2 int) STORED AS carbondata")
    sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartition options('bad_records_action'='force')")
    sql("select count(*) from badrecordsPartition").collect()
    checkAnswer(sql("select count(*) cnt from badrecordsPartition where intfield2 is null"), Seq(Row(9)))
    checkAnswer(sql("select count(*) cnt from badrecordsPartition where intfield2 is not null"), Seq(Row(2)))
  }

  test("badrecords fail on partition column") {
    sql("create table badrecordsPartitionfail(intField1 int, stringField1 string) partitioned by (intField2 int) STORED AS carbondata")
    intercept[Exception] {
      sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartitionfail options('bad_records_action'='fail')")

    }
  }

  test("badrecords ignore on partition column") {
    sql("create table badrecordsPartitionignore(intField1 int, stringField1 string) partitioned by (intField2 int) STORED AS carbondata")
    sql("create table badrecordsignore(intField1 int,intField2 int, stringField1 string) STORED AS carbondata")
    sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartitionignore options('bad_records_action'='ignore')")
    sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsignore options('bad_records_action'='ignore')")
    checkAnswer(sql("select count(*) cnt from badrecordsPartitionignore where intfield2 is null"), sql("select count(*) cnt from badrecordsignore where intfield2 is null"))
    checkAnswer(sql("select count(*) cnt from badrecordsPartitionignore where intfield2 is not null"), sql("select count(*) cnt from badrecordsignore where intfield2 is not null"))
  }


  test("test partition fails on int null partition") {
    sql("create table badrecordsPartitionintnull(intField1 int, stringField1 string) partitioned by (intField2 int) STORED AS carbondata")
    sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartitionintnull options('bad_records_action'='force')")
    checkAnswer(sql("select count(*) cnt from badrecordsPartitionintnull where intfield2 = 13"), Seq(Row(1)))
  }

  test("test partition fails on int null partition read alternate") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_READ_PARTITION_HIVE_DIRECT, "false")
    sql("create table badrecordsPartitionintnullalt(intField1 int, stringField1 string) partitioned by (intField2 int) STORED AS carbondata")
    sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartitionintnullalt options('bad_records_action'='force')")
    checkAnswer(sql("select count(*) cnt from badrecordsPartitionintnullalt where intfield2 = 13"), Seq(Row(1)))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_READ_PARTITION_HIVE_DIRECT, CarbonCommonConstants.CARBON_READ_PARTITION_HIVE_DIRECT_DEFAULT)
  }

  test("static column partition with load command") {
    sql(
      """
        | CREATE TABLE staticpartitionload (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp,attendance int,
        |  deptname String,projectcode int,
        |  utilization int,salary int,projectenddate Date,doj Timestamp)
        | PARTITIONED BY (empname String)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE staticpartitionload partition(empname='ravi') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    val frame = sql("select empno,empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from staticpartitionload")
    verifyPartitionInfo(frame, Seq("empname=ravi"))

  }

test("Creation of partition table should fail if the colname in table schema and partition column is same even if both are case sensitive") {
  val exception = intercept[Exception]{
    sql("CREATE TABLE uniqdata_char2(name char(10),id int) partitioned by (NAME char(10))STORED AS carbondata ")
  }
  assert(exception.getMessage.contains("Found duplicate column(s) in the table definition of `default`.`uniqdata_char2`: `name`"))
}

  test("Creation of partition table should fail for both spark version with same exception when char data type is created with specified digit and colname in table schema and partition column is same even if both are case sensitive") {

    sql("DROP TABLE IF EXISTS UNIQDATA_CHAR2")
    val exception = intercept[Exception]{
      sql("CREATE TABLE uniqdata_char2(name char(10),id int) partitioned by (NAME char(10))STORED AS carbondata ")
    }
    assert(exception.getMessage.contains("Found duplicate column(s) in the table definition of `default`.`uniqdata_char2`: `name`"))
  }

  test("Renaming a partition table should fail") {
    sql("drop table if exists partitionTable")
    sql(
      """create table partitionTable (id int,name String) partitioned by(email string) STORED AS carbondata
      """.stripMargin)
    sql("insert into partitionTable select 1,'huawei','abc'")
    checkAnswer(sql("show partitions partitionTable"), Seq(Row("email=abc")))
    intercept[Exception]{
      sql("alter table partitionTable PARTITION (email='abc') rename to PARTITION (email='def)")
    }
  }


  test("add partition based on location on partition table") {
    sql("drop table if exists partitionTable")
    sql(
      """create table partitionTable (id int,name String) partitioned by(email string) STORED AS carbondata
      """.stripMargin)
    sql("insert into partitionTable select 1,'huawei','abc'")
    val location = target + "/" + "def"
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(location))
    checkAnswer(sql("show partitions partitionTable"), Seq(Row("email=abc")))
    sql(s"""alter table partitionTable add partition (email='def') location '$location'""")
    sql("insert into partitionTable select 1,'huawei','def'")
    checkAnswer(sql("select email from partitionTable"), Seq(Row("def"), Row("abc")))
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(location))
  }

  test("sdk write and add partition based on location on partition table") {
    sql("drop table if exists partitionTable")
    sql("create table partitionTable (id int,name String) partitioned by(email string) stored as carbondata")
    sql("insert into partitionTable select 1,'blue','abc'")
    val schemaFile =
      CarbonTablePath.getSchemaFilePath(
        CarbonEnv.getCarbonTable(None, "partitionTable")(sqlContext.sparkSession).getTablePath)

    val sdkWritePath = target + "/" + "def"
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(sdkWritePath))

    (1 to 3).foreach { i =>
      val writer = CarbonWriter.builder()
        .outputPath(sdkWritePath)
        .writtenBy("test")
        .withSchemaFile(schemaFile)
        .withCsvInput()
        .build()
      writer.write(Seq("2", "red", "def").toArray)
      writer.write(Seq("3", "black", "def").toArray)
      writer.close()
    }

    sql(s"alter table partitionTable add partition (email='def') location '$sdkWritePath'")
    sql("show partitions partitionTable").collect()
    checkAnswer(sql("show partitions partitionTable"), Seq(Row("email=abc"), Row("email=def")))
    checkAnswer(sql("select email from partitionTable"), Seq(Row("abc"), Row("def"), Row("def"), Row("def"), Row("def"), Row("def"), Row("def")))
    checkAnswer(sql("select count(*) from partitionTable"), Seq(Row(7)))
    checkAnswer(sql("select id from partitionTable where email = 'def'"), Seq(Row(2), Row(3), Row(2), Row(3), Row(2), Row(3)))
    // alter table add partition should merge index files
    assert(FileFactory.getCarbonFile(sdkWritePath)
      .listFiles()
      .exists(_.getName.contains(".carbonindexmerge")))

    // do compaction to sort data written by sdk
    sql("alter table partitionTable compact 'major'")
    assert(sql("show segments for table partitionTable").collectAsList().get(0).getString(1).contains("Compacted"))
    checkAnswer(sql("show partitions partitionTable"), Seq(Row("email=abc"), Row("email=def")))
    checkAnswer(sql("select email from partitionTable"), Seq(Row("abc"), Row("def"), Row("def"), Row("def"), Row("def"), Row("def"), Row("def")))
    checkAnswer(sql("select count(*) from partitionTable"), Seq(Row(7)))
    checkAnswer(sql("select id from partitionTable where email = 'def'"), Seq(Row(2), Row(3), Row(2), Row(3), Row(2), Row(3)))

    sql("drop table if exists partitionTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(sdkWritePath))
  }

  test("add partition with static column partition with load command") {
    sql(
      """
        | CREATE TABLE staticpartitionlocload (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp,attendance int,
        |  deptname String,projectcode int,
        |  utilization int,salary int,projectenddate Date,doj Timestamp)
        | PARTITIONED BY (empname String)
        | STORED AS carbondata
      """.stripMargin)
    val location = target + "/" + "ravi"
    sql(s"""alter table staticpartitionlocload add partition (empname='ravi') location '$location'""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE staticpartitionlocload partition(empname='ravi') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    val frame = sql("select count(empno) from staticpartitionlocload")
    verifyPartitionInfo(frame, Seq("empname=ravi"))
    checkAnswer(sql("select count(empno) from staticpartitionlocload"), Seq(Row(10)))
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE staticpartitionlocload partition(empname='ravi') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(sql("select count(empno) from staticpartitionlocload"), Seq(Row(20)))
    val file = FileFactory.getCarbonFile(location)
    assert(file.exists())
    FileFactory.deleteAllCarbonFilesOfDir(file)
  }

  test("set partition location with static column partition with load command") {
    sql("drop table if exists staticpartitionsetloc")
    sql(
      """
        | CREATE TABLE staticpartitionsetloc (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp,attendance int,
        |  deptname String,projectcode int,
        |  utilization int,salary int,projectenddate Date,doj Timestamp)
        | PARTITIONED BY (empname String)
        | STORED AS carbondata
      """.stripMargin)
    val location = target + "/" + "ravi1"
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE staticpartitionsetloc partition(empname='ravi') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    intercept[Exception] {
      sql(s"""alter table staticpartitionsetloc partition (empname='ravi') set location '$location'""")
    }
    val file = FileFactory.getCarbonFile(location)
    FileFactory.deleteAllCarbonFilesOfDir(file)
  }

  test("add external partition with static column partition with load command with diffrent schema") {

    sql(
      """
        | CREATE TABLE staticpartitionlocloadother (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp,attendance int,
        |  deptname String,projectcode int,
        |  utilization int,salary int,projectenddate Date,doj Timestamp)
        | PARTITIONED BY (empname String)
        | STORED AS carbondata
      """.stripMargin)
    val location = target + "/" + "ravi"
    sql(s"""alter table staticpartitionlocloadother add partition (empname='ravi') location '$location'""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE staticpartitionlocloadother partition(empname='ravi') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE staticpartitionlocloadother partition(empname='indra') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(
      """
        | CREATE TABLE staticpartitionextlocload (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp,attendance int,
        |  deptname String,projectcode int,
        |  utilization int,salary int,projectenddate Date,doj Timestamp)
        | PARTITIONED BY (empname String)
        | STORED AS carbondata
      """.stripMargin)
    intercept[Exception] {
      sql(s"""alter table staticpartitionextlocload add partition (empname='ravi') location '$location'""")
    }
    assert(sql(s"show partitions staticpartitionextlocload").count() == 0)
    val file = FileFactory.getCarbonFile(location)
    if(file.exists()) {
      FileFactory.deleteAllCarbonFilesOfDir(file)
    }
  }

  test("add external partition with static column partition with load command") {
    sql(
      """
        | CREATE TABLE staticpartitionlocloadother_new (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp,attendance int,
        |  deptname String,projectcode int,
        |  utilization int,salary int,projectenddate Date,doj Timestamp)
        | PARTITIONED BY (empname String)
        | STORED AS carbondata
      """.stripMargin)
    val location = target + "/" + "ravi1"
    try {
      sql(s"""alter table staticpartitionlocloadother_new add partition (empname='ravi') location '$location'""")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE staticpartitionlocloadother_new partition(empname='ravi') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE staticpartitionlocloadother_new partition(empname='indra') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
      checkAnswer(sql(s"select count(deptname) from staticpartitionlocloadother_new"), Seq(Row(20)))
      sql(s"""ALTER TABLE staticpartitionlocloadother_new DROP PARTITION(empname='ravi')""")
      checkAnswer(sql(s"select count(deptname) from staticpartitionlocloadother_new"), Seq(Row(10)))
      sql(s"""alter table staticpartitionlocloadother_new add partition (empname='ravi') location '$location'""")
      checkAnswer(sql(s"select count(deptname) from staticpartitionlocloadother_new"), Seq(Row(20)))
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE staticpartitionlocloadother_new partition(empname='ravi') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
      checkAnswer(sql(s"select count(deptname) from staticpartitionlocloadother_new"), Seq(Row(30)))
    } finally {
      val file = FileFactory.getCarbonFile(location)
      if(file.exists()) {
        FileFactory.deleteAllCarbonFilesOfDir(file)
      }
    }
  }

  test("validate data in partition table after dropping and adding a column") {
    sql("drop table if exists par")
    sql("create table par(name string, add string) partitioned by (age double) " +
              "STORED AS carbondata TBLPROPERTIES('cache_level'='blocklet')")
    sql("insert into par select 'joey','NY',32 union all select 'chandler','NY',32")
    sql("alter table par drop columns(name)")
    sql("alter table par add columns(name string)")
    sql("insert into par select 'joey','NY',32 union all select 'joey','NY',32")
    checkAnswer(sql("select name from par"), Seq(Row("NY"), Row("NY"), Row(null), Row(null)))
    sql("drop table if exists par")
  }

  test("test drop column when after dropping only partition column remains and datatype change on partition column") {
    sql("drop table if exists onlyPart")
    sql("create table onlyPart(name string) partitioned by (age int) " +
        "STORED AS carbondata TBLPROPERTIES('cache_level'='blocklet')")
    val ex1 = intercept[MalformedCarbonCommandException] {
      sql("alter table onlyPart drop columns(name)")
    }
    assert(ex1.getMessage.contains("alter table drop column is failed, cannot have the table with all columns as partition column"))
    val ex2 = intercept[MalformedCarbonCommandException] {
      sql("alter table onlyPart change age age bigint")
    }
    assert(ex2.getMessage.contains("Alter on partition column age is not supported"))
    sql("drop table if exists onlyPart")
  }

  if (CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_SPARK_VERSION_SPARK3,
      CarbonCommonConstants.CARBON_SPARK_VERSION_SPARK3_DEFAULT).toBoolean) {
    test("test create partition on existing table columns") {
      sql("drop table if exists partitionTable")
      sql("create table partitionTable(c1 int, c2 int, v1 string, v2 string) " +
          "stored as carbondata partitioned by (v2,c2)")
      val descTable = sql(s"describe formatted partitionTable").collect
      descTable.find(_.get(0).toString.contains("Partition Columns")) match {
        case Some(row) => assert(row.get(1).toString.contains("v2:STRING, c2:INT"))
        case None => assert(false)
      }
      sql("insert into partitionTable select 1,'sd','sd',2")
      sql("alter table partitionTable add partition (v2='ke', c2=3) location 'loc1'")
      checkAnswer(sql("show partitions partitionTable"),
        Seq(Row("v2=sd/c2=2"), Row("v2=ke/c2=3")))
      checkAnswer(sql("select *from partitionTable"), Seq(Row(1, "sd", "sd", 2)))
      sql("drop table if exists partitionTable")
    }
  }

  private def verifyPartitionInfo(frame: DataFrame, partitionNames: Seq[String]) = {
    val plan = frame.queryExecution.sparkPlan
    val scanRDD = plan collect {
      case b: CarbonDataSourceScan
        if b.inputRDDs().head.isInstanceOf[CarbonScanRDD[InternalRow]] =>
        b.inputRDDs().head.asInstanceOf[CarbonScanRDD[InternalRow]]
    }
    assert(scanRDD.nonEmpty)
    assert(!partitionNames.map(f => scanRDD.head.partitionNames.exists(_.getPartitions.contains(f))).exists(!_))
  }

  override def afterAll: Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    dropTable
  }

  private def dropTable = {
    sql("drop table if exists partitionxxx")
    sql("drop table if exists originTable")
    sql("drop table if exists originMultiLoads")
    sql("drop table if exists partitionone")
    sql("drop table if exists partitiontwo")
    sql("drop table if exists partitionmany")
    sql("drop table if exists partitiondate")
    sql("drop table if exists partitiondateinsert")
    sql("drop table if exists badrecordsPartition")
    sql("drop table if exists staticpartitionload")
    sql("drop table if exists badrecordsPartitionignore")
    sql("drop table if exists badrecordsPartitionfail")
    sql("drop table if exists badrecordsignore")
    sql("drop table if exists badrecordsPartitionintnull")
    sql("drop table if exists badrecordsPartitionintnullalt")
    sql("drop table if exists partitionTable")
    sql("drop table if exists staticpartitionlocload")
    sql("drop table if exists staticpartitionextlocload")
    sql("drop table if exists staticpartitionlocloadother")
    sql("drop table if exists staticpartitionextlocload_new")
    sql("drop table if exists staticpartitionlocloadother_new")
    sql("drop table if exists par")
    sql("drop table if exists onlyPart")
  }
  // scalastyle:on lineLength
}

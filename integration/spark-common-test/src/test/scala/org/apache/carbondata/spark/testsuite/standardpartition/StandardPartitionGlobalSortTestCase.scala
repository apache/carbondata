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

import java.util
import java.util.concurrent.{Callable, ExecutorService, Executors}

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, Ignore}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

class StandardPartitionGlobalSortTestCase extends QueryTest with BeforeAndAfterAll {
  var executorService: ExecutorService = _
  override def beforeAll {
    dropTable
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    sql(
      """
        | CREATE TABLE originTable (empno int, empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE originTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
  }

  def validateDataFiles(tableUniqueName: String, segmentId: String, partitions: Int): Unit = {
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(tableUniqueName)
    val tablePath = new CarbonTablePath(carbonTable.getCarbonTableIdentifier,
      carbonTable.getTablePath)
    val segmentDir = tablePath.getCarbonDataDirectoryPath("0", segmentId)
    val carbonFile = FileFactory.getCarbonFile(segmentDir, FileFactory.getFileType(segmentDir))
    val dataFiles = carbonFile.listFiles(new CarbonFileFilter() {
      override def accept(file: CarbonFile): Boolean = {
        return file.getName.endsWith(".partitionmap")
      }
    })
    assert(dataFiles.length == partitions)
  }

  test("data loading for global sort partition table for one partition column") {
    sql(
      """
        | CREATE TABLE partitionone (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionone OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"', 'GLOBAL_SORT_PARTITIONS'='1')""")

    validateDataFiles("default_partitionone", "0", 1)

    checkAnswer(sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from partitionone order by empno"),
      sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable order by empno"))

  }

  test("data loading for global partition table for two partition column") {
    sql(
      """
        | CREATE TABLE partitiontwo (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (doj Timestamp, empname String)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiontwo OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    validateDataFiles("default_partitiontwo", "0", 1)

    checkAnswer(sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from partitiontwo order by empno"),
      sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable order by empno"))

  }

  test("data loading for global sort partition table for one static partition column") {
    sql(
      """
        | CREATE TABLE staticpartitionone (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""insert into staticpartitionone PARTITION(empno='1') select empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary from originTable""")

    validateDataFiles("default_staticpartitionone", "0", 1)
  }

  test("single pass loading for global sort partition table for one partition column") {
    sql(
      """
        | CREATE TABLE singlepasspartitionone (empname String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (designation String)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE singlepasspartitionone OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"', 'SINGLE_PASS'='true')""")

    validateDataFiles("default_singlepasspartitionone", "0", 1)
  }

  test("data loading for global sort partition table for one static partition column with load syntax") {
    sql(
      """
        | CREATE TABLE loadstaticpartitionone (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE loadstaticpartitionone PARTITION(empno='1') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    checkAnswer(sql("select distinct empno from loadstaticpartitionone"), Seq(Row(1)))
  }

  test("overwrite global sort partition table for one static partition column with load syntax") {
    sql(
      """
        | CREATE TABLE loadstaticpartitiononeoverwrite (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE loadstaticpartitiononeoverwrite PARTITION(empno='1') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    val rows = sql("select count(*) from loadstaticpartitiononeoverwrite").collect()

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE loadstaticpartitiononeoverwrite PARTITION(empno='1') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE loadstaticpartitiononeoverwrite PARTITION(empno='1') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE loadstaticpartitiononeoverwrite PARTITION(empno='1') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    checkAnswer(sql("select count(*) from loadstaticpartitiononeoverwrite"), rows)
  }

  test("test global sort partition column with special characters") {
    sql(
      """
        | CREATE TABLE loadpartitionwithspecialchar (empno int, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empname String)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data_with_special_char.csv' INTO TABLE loadpartitionwithspecialchar OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    checkAnswer(sql("select count(*) from loadpartitionwithspecialchar"), Seq(Row(10)))
    checkAnswer(sql("select count(*) from loadpartitionwithspecialchar where empname='sibi=56'"), Seq(Row(1)))
    checkAnswer(sql("select count(*) from loadpartitionwithspecialchar where empname='arvind,ss'"), Seq(Row(1)))
  }

  test("concurrent global sort partition table load test") {
    executorService = Executors.newCachedThreadPool()
    sql(
      """
        | CREATE TABLE partitionmultiplethreeconcurrent (empno int, doj Timestamp,
        |  workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (workgroupcategory int, empname String, designation String)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('DICTIONARY_INCLUDE'='empname,designation,deptname', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)

    val tasks = new util.ArrayList[Callable[String]]()
    tasks.add(new QueryTask(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionmultiplethreeconcurrent OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')"""))
    tasks.add(new QueryTask(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionmultiplethreeconcurrent OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')"""))
    tasks.add(new QueryTask(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionmultiplethreeconcurrent OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')"""))
    val results = executorService.invokeAll(tasks)
    for (i <- 0 until tasks.size()) {
      val res = results.get(i).get
      assert("PASS".equals(res))
    }
    executorService.shutdown()
    checkAnswer(sql("select count(*) from partitionmultiplethreeconcurrent"), Seq(Row(30)))
  }

  class QueryTask(query: String) extends Callable[String] {
    override def call(): String = {
      var result = "PASS"
      try {
        LOGGER.info("Executing :" + Thread.currentThread().getName)
        sql(query)
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          result = "FAIL"
      }
      result
    }
  }

  test("global sort bad record test with null values") {
    sql(s"""CREATE TABLE IF NOT EXISTS emp1 (emp_no int,ename string,job string,mgr_id int,date_of_joining string,salary int,bonus int) partitioned by (dept_no int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""LOAD DATA INPATH '$resourcesPath/emp.csv' overwrite INTO TABLE emp1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '\')""")
    val rows = sql(s"select count(*) from emp1").collect()
    sql(s"""LOAD DATA INPATH '$resourcesPath/emp.csv' overwrite INTO TABLE emp1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '\','BAD_RECORDS_ACTION'='FORCE')""")
    checkAnswer(sql(s"select count(*) from emp1"), rows)
  }

  test("global sort badrecords on partition column") {
    sql("create table badrecordsPartition(intField1 int, stringField1 string) partitioned by (intField2 int) stored by 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')")
    sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartition options('bad_records_action'='force')")
    sql("select count(*) from badrecordsPartition").show()
    checkAnswer(sql("select count(*) cnt from badrecordsPartition where intfield2 is null"), Seq(Row(9)))
    checkAnswer(sql("select count(*) cnt from badrecordsPartition where intfield2 is not null"), Seq(Row(2)))
  }

  test("global sort badrecords fail on partition column") {
    sql("create table badrecordsPartitionfail(intField1 int, stringField1 string) partitioned by (intField2 int) stored by 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')")
    intercept[Exception] {
      sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartitionfail options('bad_records_action'='fail')")

    }
  }

  test("global sort badrecords ignore on partition column") {
    sql("create table badrecordsPartitionignore(intField1 int, stringField1 string) partitioned by (intField2 int) stored by 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')")
    sql("create table badrecordsignore(intField1 int,intField2 int, stringField1 string) stored by 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')")
    sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartitionignore options('bad_records_action'='ignore')")
    sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsignore options('bad_records_action'='ignore')")
    checkAnswer(sql("select count(*) cnt from badrecordsPartitionignore where intfield2 is null"), sql("select count(*) cnt from badrecordsignore where intfield2 is null"))
    checkAnswer(sql("select count(*) cnt from badrecordsPartitionignore where intfield2 is not null"), sql("select count(*) cnt from badrecordsignore where intfield2 is not null"))
  }


  test("global sort test partition fails on int null partition") {
    sql("create table badrecordsPartitionintnull(intField1 int, stringField1 string) partitioned by (intField2 int) stored by 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')")
    sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartitionintnull options('bad_records_action'='force')")
    checkAnswer(sql("select count(*) cnt from badrecordsPartitionintnull where intfield2 = 13"), Seq(Row(1)))
  }

  test("global sort test partition fails on int null partition read alternate") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_READ_PARTITION_HIVE_DIRECT, "false")
    sql("create table badrecordsPartitionintnullalt(intField1 int, stringField1 string) partitioned by (intField2 int) stored by 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')")
    sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartitionintnullalt options('bad_records_action'='force')")
    checkAnswer(sql("select count(*) cnt from badrecordsPartitionintnullalt where intfield2 = 13"), Seq(Row(1)))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_READ_PARTITION_HIVE_DIRECT, CarbonCommonConstants.CARBON_READ_PARTITION_HIVE_DIRECT_DEFAULT)
  }

  test("global sort static column partition with load command") {
    sql(
      """
        | CREATE TABLE staticpartitionload (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp,attendance int,
        |  deptname String,projectcode int,
        |  utilization int,salary int,projectenddate Date,doj Timestamp)
        | PARTITIONED BY (empname String)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE staticpartitionload partition(empname='ravi') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    val frame = sql("select empno,empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from staticpartitionload")
    checkExistence(sql(s"""SHOW PARTITIONS staticpartitionload"""), true, "empname=ravi")
  }

  test("overwriting global sort static partition table for date partition column on insert query") {
    sql(
      """
        | CREATE TABLE staticpartitiondateinsert (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp,attendance int,
        |  deptname String,projectcode int,
        |  utilization int,salary int)
        | PARTITIONED BY (projectenddate Date,doj Timestamp)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""insert into staticpartitiondateinsert select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable""")
    sql(s"""insert into staticpartitiondateinsert select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable""")
    sql(s"""insert into staticpartitiondateinsert select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable""")
    sql(s"""insert into staticpartitiondateinsert select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable""")
    sql(s"""insert overwrite table staticpartitiondateinsert PARTITION(projectenddate='2016-06-29',doj='2010-12-29 00:00:00') select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary from originTable where projectenddate=cast('2016-06-29' as Date)""")
    //    sql(s"""insert overwrite table partitiondateinsert  select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable""")
    checkAnswer(sql("select * from staticpartitiondateinsert where projectenddate=cast('2016-06-29' as Date)"),
      sql("select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable where projectenddate=cast('2016-06-29' as Date)"))
  }


  test("dynamic and static global sort partition table with load syntax") {
    sql(
      """
        | CREATE TABLE loadstaticpartitiondynamic (designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int, empname String)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE loadstaticpartitiondynamic PARTITION(empno='1', empname) OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(sql(s"select count(*) from loadstaticpartitiondynamic where empno=1"), sql(s"select count(*) from loadstaticpartitiondynamic"))
  }

  test("dynamic and static global sort partition table with overwrite ") {
    sql(
      """
        | CREATE TABLE insertstaticpartitiondynamic (designation String, doj Timestamp,salary int)
        | PARTITIONED BY (empno int, empname String)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE insertstaticpartitiondynamic PARTITION(empno, empname) OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    val rows = sql(s"select count(*) from insertstaticpartitiondynamic").collect()
    sql("""insert overwrite table insertstaticpartitiondynamic PARTITION(empno='1', empname) select designation, doj, salary, empname from insertstaticpartitiondynamic""")

    checkAnswer(sql(s"select count(*) from insertstaticpartitiondynamic where empno=1"), rows)

    intercept[Exception] {
      sql("""insert overwrite table insertstaticpartitiondynamic PARTITION(empno, empname='ravi') select designation, doj, salary, empname from insertstaticpartitiondynamic""")
    }

  }

  test("overwriting global sort all partition on table and do compaction") {
    sql(
      """
        | CREATE TABLE partitionallcompaction (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (deptname String,doj Timestamp,projectcode int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE partitionallcompaction OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE partitionallcompaction PARTITION(deptname='Learning', doj, projectcode) OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"') """)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE partitionallcompaction PARTITION(deptname='configManagement', doj, projectcode) OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE partitionallcompaction PARTITION(deptname='network', doj, projectcode) OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE partitionallcompaction PARTITION(deptname='protocol', doj, projectcode) OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE partitionallcompaction PARTITION(deptname='security', doj, projectcode) OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql("ALTER TABLE partitionallcompaction COMPACT 'MAJOR'").collect()
    checkExistence(sql(s"""SHOW segments for table partitionallcompaction"""), true, "Marked for Delete")
  }

  test("Test global sort overwrite static partition ") {
    sql(
      """
        | CREATE TABLE weather6 (type String)
        | PARTITIONED BY (year int, month int, day int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)

    sql("insert into weather6 partition(year=2014, month=5, day=25) select 'rainy'")
    sql("insert into weather6 partition(year=2014, month=4, day=23) select 'cloudy'")
    sql("insert overwrite table weather6 partition(year=2014, month=5, day=25) select 'sunny'")
    checkExistence(sql("select * from weather6"), true, "sunny")
    checkAnswer(sql("select count(*) from weather6"), Seq(Row(2)))
  }

  test("Test global sort overwrite static partition with wrong int value") {
    sql(
      """
        | CREATE TABLE weather7 (type String)
        | PARTITIONED BY (year int, month int, day int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)

    sql("insert into weather7 partition(year=2014, month=05, day=25) select 'rainy'")
    sql("insert into weather7 partition(year=2014, month=04, day=23) select 'cloudy'")
    sql("insert overwrite table weather7 partition(year=2014, month=05, day=25) select 'sunny'")
    checkExistence(sql("select * from weather7"), true, "sunny")
    checkAnswer(sql("select count(*) from weather7"), Seq(Row(2)))
    sql("insert into weather7 partition(year=2014, month, day) select 'rainy1',06,25")
    sql("insert into weather7 partition(year=2014, month=01, day) select 'rainy2',27")
    sql("insert into weather7 partition(year=2014, month=01, day=02) select 'rainy3'")
    checkAnswer(sql("select count(*) from weather7 where month=1"), Seq(Row(2)))
  }


  test("test overwrite missed scenarios") {
    sql(s"""create table carbon_test(
      id string,
      name string
      )
      PARTITIONED BY(record_date int)
      STORED BY 'org.apache.carbondata.format'
      TBLPROPERTIES('SORT_COLUMNS'='id')""")
    sql(s"""create table carbon_test_hive(
      id string,
      name string
      )
      PARTITIONED BY(record_date int)""")
    sql(s"""set hive.exec.dynamic.partition.mode=nonstrict""")
    sql(s"""insert overwrite table carbon_test partition(record_date) select '1','kim',unix_timestamp('2018-02-05','yyyy-MM-dd') as record_date""")
    sql(s"""insert overwrite table carbon_test_hive partition(record_date) select '1','kim',unix_timestamp('2018-02-05','yyyy-MM-dd') as record_date""")

    checkAnswer(sql(s"""select * from carbon_test where record_date=1517817600"""), sql(s"""select * from carbon_test_hive where record_date=1517817600"""))
    sql(s"""insert overwrite table carbon_test partition(record_date) select '1','kim1',unix_timestamp('2018-02-06','yyyy-MM-dd') as record_date """)
    sql(s"""insert overwrite table carbon_test_hive partition(record_date) select '1','kim1',unix_timestamp('2018-02-06','yyyy-MM-dd') as record_date """)

    checkAnswer(sql(s"""select * from carbon_test where record_date=1517817600"""), sql(s"""select * from carbon_test_hive where record_date=1517817600"""))
    checkAnswer(sql(s"""select * from carbon_test where record_date=1517904000"""), sql(s"""select * from carbon_test_hive where record_date=1517904000"""))
    sql(s"""insert overwrite table carbon_test partition(record_date) select '1','kim2',unix_timestamp('2018-02-07','yyyy-MM-dd') as record_date""")
    sql(s"""insert overwrite table carbon_test_hive partition(record_date) select '1','kim2',unix_timestamp('2018-02-07','yyyy-MM-dd') as record_date""")

    checkAnswer(sql(s"""select * from carbon_test where record_date=1517817600"""), sql(s"""select * from carbon_test_hive where record_date=1517817600"""))
    checkAnswer(sql(s"""select * from carbon_test where record_date=1517904000"""), sql(s"""select * from carbon_test_hive where record_date=1517904000"""))
    checkAnswer(sql(s"""select * from carbon_test where record_date=1517990400"""), sql(s"""select * from carbon_test_hive where record_date=1517990400"""))
  }

  test("test overwrite with timestamp partition column") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql("DROP TABLE IF EXISTS origintable")
    sql(
      """
        | CREATE TABLE origintable
        | (id Int,
        | vin String,
        | logdate Timestamp,
        | phonenumber Long,
        | country String,
        | area String,
        | salary Int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(
      s"""
       LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' into table origintable
       """)

    sql("DROP TABLE IF EXISTS partitiontable0")
    sql("DROP TABLE IF EXISTS partitiontable0_hive")
    sql(
      """
        | CREATE TABLE partitiontable0
        | (id Int,
        | vin String,
        | phonenumber Long,
        | country String,
        | area String,
        | salary Int)
        | PARTITIONED BY (logdate Timestamp)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='id,vin')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE partitiontable0_hive
        | (id Int,
        | vin String,
        | phonenumber Long,
        | country String,
        | area String,
        | salary Int)
        | PARTITIONED BY (logdate Timestamp)
      """.stripMargin)
    sql(s"""set hive.exec.dynamic.partition.mode=nonstrict""")

    sql(
      s"""
       LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' into table partitiontable0
       """)

    sql(
      s"""
       insert into partitiontable0_hive select * from partitiontable0
       """)

    checkAnswer(sql(
      s"""
         | SELECT logdate,id,vin,phonenumber,country,area,salary
         | FROM partitiontable0 where logdate = '2016-02-12'
      """.stripMargin), sql(
      s"""
         | SELECT logdate,id,vin,phonenumber,country,area,salary
         | FROM partitiontable0_hive where logdate = '2016-02-12'
      """.stripMargin))

    sql("insert into table partitiontable0 partition(logdate='2018-02-15 00:00:00') " +
              "select id,vin,phonenumber,country,area,salary from origintable")
    sql("insert into table partitiontable0_hive partition(logdate='2018-02-15 00:00:00') " +
        "select id,vin,phonenumber,country,area,salary from origintable")
    checkAnswer(sql(
      s"""
         | SELECT logdate,id,vin,phonenumber,country,area,salary
         | FROM partitiontable0 where logdate = '2018-02-15'
      """.stripMargin), sql(
      s"""
         | SELECT logdate,id,vin,phonenumber,country,area,salary
         | FROM partitiontable0_hive where logdate = '2018-02-15'
      """.stripMargin))

    checkAnswer(sql(
      s"""
         | SELECT count(*) FROM partitiontable0""".stripMargin), sql(
      s"""
         | SELECT count(*) FROM partitiontable0_hive""".stripMargin))

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }

  test("test overwrite with date partition column") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
    sql("DROP TABLE IF EXISTS origintable")
    sql(
      """
        | CREATE TABLE origintable
        | (id Int,
        | vin String,
        | logdate date,
        | phonenumber Long,
        | country String,
        | area String,
        | salary Int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(
      s"""
       LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' into table origintable
       """)

    sql("DROP TABLE IF EXISTS partitiontable0")
    sql("DROP TABLE IF EXISTS partitiontable0_hive")
    sql(
      """
        | CREATE TABLE partitiontable0
        | (id Int,
        | vin String,
        | phonenumber Long,
        | country String,
        | area String,
        | salary Int)
        | PARTITIONED BY (logdate date)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='id,vin')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE partitiontable0_hive
        | (id Int,
        | vin String,
        | phonenumber Long,
        | country String,
        | area String,
        | salary Int)
        | PARTITIONED BY (logdate date)
      """.stripMargin)
    sql(s"""set hive.exec.dynamic.partition.mode=nonstrict""")

    sql(
      s"""
       LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' into table partitiontable0
       """)

    sql(
      s"""
       insert into partitiontable0_hive select * from partitiontable0
       """)

    checkAnswer(sql(
      s"""
         | SELECT logdate,id,vin,phonenumber,country,area,salary
         | FROM partitiontable0 where logdate = '2016-02-12'
      """.stripMargin), sql(
      s"""
         | SELECT logdate,id,vin,phonenumber,country,area,salary
         | FROM partitiontable0_hive where logdate = '2016-02-12'
      """.stripMargin))

    sql("insert into table partitiontable0 partition(logdate='2018-02-15') " +
        "select id,vin,phonenumber,country,area,salary from origintable")
    sql("insert into table partitiontable0_hive partition(logdate='2018-02-15') " +
        "select id,vin,phonenumber,country,area,salary from origintable")
    checkAnswer(sql(
      s"""
         | SELECT logdate,id,vin,phonenumber,country,area,salary
         | FROM partitiontable0 where logdate = '2018-02-15'
      """.stripMargin), sql(
      s"""
         | SELECT logdate,id,vin,phonenumber,country,area,salary
         | FROM partitiontable0_hive where logdate = '2018-02-15'
      """.stripMargin))

    checkAnswer(sql(
      s"""
         | SELECT count(*) FROM partitiontable0""".stripMargin), sql(
      s"""
         | SELECT count(*) FROM partitiontable0_hive""".stripMargin))

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd-MM-yyyy")
  }



  override def afterAll = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TASK_DISTRIBUTION ,
      CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_DEFAULT)
//    dropTable
    if (executorService != null && !executorService.isShutdown) {
      executorService.shutdownNow()
    }
  }

  def dropTable = {
    sql("drop table if exists originTable")
    sql("drop table if exists originMultiLoads")
    sql("drop table if exists partitionone")
    sql("drop table if exists partitiontwo")
    sql("drop table if exists partitionthree")
    sql("drop table if exists partitionmultiplethree")
    sql("drop table if exists insertpartitionthree")
    sql("drop table if exists staticpartitionone")
    sql("drop table if exists singlepasspartitionone")
    sql("drop table if exists loadstaticpartitionone")
    sql("drop table if exists loadstaticpartitiononeoverwrite")
    sql("drop table if exists streamingpartitionedtable")
    sql("drop table if exists mergeindexpartitionthree")
    sql("drop table if exists loadstaticpartitiononeissue")
    sql("drop table if exists partitionmultiplethreeconcurrent")
    sql("drop table if exists loadpartitionwithspecialchar")
    sql("drop table if exists emp1")
    sql("drop table if exists restorepartition")
    sql("drop table if exists casesensitivepartition")
    sql("drop table if exists badrecordsPartition")
    sql("drop table if exists staticpartitionload")
    sql("drop table if exists badrecordsPartitionignore")
    sql("drop table if exists badrecordsPartitionfail")
    sql("drop table if exists badrecordsignore")
    sql("drop table if exists badrecordsPartitionintnull")
    sql("drop table if exists badrecordsPartitionintnullalt")
    sql("drop table if exists partitiondateinsert")
    sql("drop table if exists staticpartitiondateinsert")
    sql("drop table if exists loadstaticpartitiondynamic")
    sql("drop table if exists insertstaticpartitiondynamic")
    sql("drop table if exists partitionallcompaction")
    sql("drop table if exists weather6")
    sql("drop table if exists weather7")
    sql("drop table if exists uniqdata_hive_static")
    sql("drop table if exists uniqdata_hive_dynamic")
    sql("drop table if exists uniqdata_string_static")
    sql("drop table if exists uniqdata_string_dynamic")
    sql("drop table if exists partitionLoadTable")
    sql("drop table if exists noLoadTable")
    sql("drop table if exists carbon_test")
    sql("drop table if exists carbon_test_hive")
  }
}

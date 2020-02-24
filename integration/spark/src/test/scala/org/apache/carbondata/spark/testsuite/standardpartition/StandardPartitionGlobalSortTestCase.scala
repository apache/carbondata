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
import org.apache.spark.util.SparkUtil
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class StandardPartitionGlobalSortTestCase extends QueryTest with BeforeAndAfterAll {
  var executorService: ExecutorService = _
  override def beforeAll {
    defaultConfig()
    dropTable
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    sql(
      """
        | CREATE TABLE originTable (empno int, empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE originTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
  }

  test("data loading for global sort partition table for one partition column") {
    sql(
      """
        | CREATE TABLE partitionone (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionone OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"', 'GLOBAL_SORT_PARTITIONS'='1')""")

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
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiontwo OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

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
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""insert into staticpartitionone PARTITION(empno='1') select empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary from originTable""")

  }

  test("data loading for global sort partition table for one static partition column with load syntax") {
    sql(
      """
        | CREATE TABLE loadstaticpartitionone (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
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
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
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
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data_with_special_char.csv' INTO TABLE loadpartitionwithspecialchar OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    checkAnswer(sql("select count(*) from loadpartitionwithspecialchar"), Seq(Row(10)))
    checkAnswer(sql("select count(*) from loadpartitionwithspecialchar where empname='sibi=56'"), Seq(Row(1)))
    checkAnswer(sql("select count(*) from loadpartitionwithspecialchar where empname='arvind,ss'"), Seq(Row(1)))
  }

  ignore("concurrent global sort partition table load test") {
    executorService = Executors.newCachedThreadPool()
    sql(
      """
        | CREATE TABLE partitionmultiplethreeconcurrent (empno int, doj Timestamp,
        |  workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int,workgroupcategory int,designation String)
        | PARTITIONED BY (empname String)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)

    val tasks = new util.ArrayList[Callable[String]]()
    var i = 0
    val count = 5
    while (i < count) {
      tasks.add(new QueryTask(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE  partitionmultiplethreeconcurrent partition(empname='ravi') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')"""))
      i = i + 1
    }
    val results = executorService.invokeAll(tasks)
    for (i <- 0 until tasks.size()) {
      val res = results.get(i).get
      assert("PASS".equals(res))
    }
    executorService.shutdown()
    checkAnswer(sql("select count(*) from partitionmultiplethreeconcurrent"), Seq(Row(10 * count)))
    assert(sql("show segments for table partitionmultiplethreeconcurrent").count() == count)
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
    sql(s"""CREATE TABLE IF NOT EXISTS emp1 (emp_no int,ename string,job string,mgr_id int,date_of_joining string,salary int,bonus int) partitioned by (dept_no int) STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""LOAD DATA INPATH '$resourcesPath/emp.csv' overwrite INTO TABLE emp1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '\')""")
    val rows = sql(s"select count(*) from emp1").collect()
    sql(s"""LOAD DATA INPATH '$resourcesPath/emp.csv' overwrite INTO TABLE emp1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '\','BAD_RECORDS_ACTION'='FORCE')""")
    checkAnswer(sql(s"select count(*) from emp1"), rows)
  }

  test("global sort badrecords on partition column") {
    sql("create table badrecordsPartition(intField1 int, stringField1 string) partitioned by (intField2 int) STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')")
    sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartition options('bad_records_action'='force')")
    sql("select count(*) from badrecordsPartition").show()
    checkAnswer(sql("select count(*) cnt from badrecordsPartition where intfield2 is null"), Seq(Row(9)))
    checkAnswer(sql("select count(*) cnt from badrecordsPartition where intfield2 is not null"), Seq(Row(2)))
  }

  test("global sort badrecords fail on partition column") {
    sql("create table badrecordsPartitionfail(intField1 int, stringField1 string) partitioned by (intField2 int) STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')")
    intercept[Exception] {
      sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartitionfail options('bad_records_action'='fail')")

    }
  }

  test("global sort badrecords ignore on partition column") {
    sql("create table badrecordsPartitionignore(intField1 int, stringField1 string) partitioned by (intField2 int) STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')")
    sql("create table badrecordsignore(intField1 int,intField2 int, stringField1 string) STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')")
    sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartitionignore options('bad_records_action'='ignore')")
    sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsignore options('bad_records_action'='ignore')")
    checkAnswer(sql("select count(*) cnt from badrecordsPartitionignore where intfield2 is null"), sql("select count(*) cnt from badrecordsignore where intfield2 is null"))
    checkAnswer(sql("select count(*) cnt from badrecordsPartitionignore where intfield2 is not null"), sql("select count(*) cnt from badrecordsignore where intfield2 is not null"))
  }


  test("global sort test partition fails on int null partition") {
    sql("create table badrecordsPartitionintnull(intField1 int, stringField1 string) partitioned by (intField2 int) STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')")
    sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartitionintnull options('bad_records_action'='force')")
    checkAnswer(sql("select count(*) cnt from badrecordsPartitionintnull where intfield2 = 13"), Seq(Row(1)))
  }

  test("global sort test partition fails on int null partition read alternate") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_READ_PARTITION_HIVE_DIRECT, "false")
    sql("create table badrecordsPartitionintnullalt(intField1 int, stringField1 string) partitioned by (intField2 int) STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')")
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
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
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
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""insert into staticpartitiondateinsert select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable""")
    sql(s"""insert into staticpartitiondateinsert select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable""")
    sql(s"""insert into staticpartitiondateinsert select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable""")
    sql(s"""insert into staticpartitiondateinsert select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable""")
    sql(s"""insert overwrite table staticpartitiondateinsert PARTITION(projectenddate='2016-06-29',doj='2010-12-29 00:00:00') select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary from originTable where projectenddate=cast('2016-06-29' as timestamp)""")
    checkAnswer(sql("select * from staticpartitiondateinsert where projectenddate=cast('2016-06-29' as Date)"),
      sql("select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,cast(projectenddate as date),doj from originTable where projectenddate=cast('2016-06-29' as timestamp)"))
  }


  test("dynamic and static global sort partition table with load syntax") {
    sql(
      """
        | CREATE TABLE loadstaticpartitiondynamic (designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int, empname String)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE loadstaticpartitiondynamic PARTITION(empno='1', empname) OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(sql(s"select count(*) from loadstaticpartitiondynamic where empno=1"), sql(s"select count(*) from loadstaticpartitiondynamic"))
  }

  test("dynamic and static global sort partition table with overwrite ") {
    sql(
      """
        | CREATE TABLE insertstaticpartitiondynamic (designation String, doj Timestamp,salary int)
        | PARTITIONED BY (empno int, empname String)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
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
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
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
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
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
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
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
      STORED AS carbondata
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
    sql("DROP TABLE IF EXISTS origintablenew")
    sql(
      """
        | CREATE TABLE origintablenew
        | (id Int,
        | vin String,
        | logdate Timestamp,
        | phonenumber Long,
        | country String,
        | area String,
        | salary Int)
        | STORED AS carbondata
      """.stripMargin)

    sql(
      s"""
       LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' into table origintablenew
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
        | STORED AS carbondata
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
              "select id,vin,phonenumber,country,area,salary from origintablenew")
    sql("insert into table partitiontable0_hive partition(logdate='2018-02-15 00:00:00') " +
        "select id,vin,phonenumber,country,area,salary from origintablenew")
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
    sql("DROP TABLE IF EXISTS origintablenew")
    sql(
      """
        | CREATE TABLE origintablenew
        | (id Int,
        | vin String,
        | logdate date,
        | phonenumber Long,
        | country String,
        | area String,
        | salary Int)
        | STORED AS carbondata
      """.stripMargin)

    sql(
      s"""
       LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' into table origintablenew
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
        | STORED AS carbondata
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
        "select id,vin,phonenumber,country,area,salary from origintablenew")
    sql("insert into table partitiontable0_hive partition(logdate='2018-02-15') " +
        "select id,vin,phonenumber,country,area,salary from origintablenew")
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

  test("partition with date column issue") {
    try {
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FAIL.name())
      sql("drop table if exists partdatecarb")
      sql(
        "create table partdatecarb(name string, age Int) partitioned by(dob date) STORED AS carbondata")

      sql("insert into partdatecarb partition(dob='2016-06-28') select 'name1',121")
      checkAnswer(sql("select name,age,cast(dob as string) from partdatecarb"),
        Seq(Row("name1", 121, "2016-06-28")))
    } finally {
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FORCE.name())
    }
  }

  test("partition with time column issue") {
    try {
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FAIL.name())
      sql("drop table if exists partdatecarb1")
      sql(
        "create table partdatecarb1(name string, age Int) partitioned by(dob timestamp) STORED AS carbondata")

      sql("insert into partdatecarb1 partition(dob='2016-06-28 00:00:00') select 'name1',121")
      checkAnswer(sql("select name,age,cast(dob as string) from partdatecarb1"),
        Seq(Row("name1", 121, "2016-06-28 00:00:00")))
    } finally {
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FORCE.name())
    }
  }

  test("partition with int issue and dictionary exclude") {
    try {
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FAIL.name())
      sql("drop table if exists partdatecarb2")
      sql(
        "create table partdatecarb2(name string, dob string) partitioned by(age Int) STORED AS carbondata ")

      sql("insert into partdatecarb2 partition(age='12') select 'name1','2016-06-28'")
      checkAnswer(sql("select name,age,cast(dob as string) from partdatecarb2"),
        Seq(Row("name1", 12, "2016-06-28")))
    } finally {
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FORCE.name())
    }
  }

  test("data loading for all dimensions with table for two partition column") {
    sql("drop table if exists partitiontwoalldims")
    sql(
      """
        | CREATE TABLE partitiontwoalldims (empno String, designation String,
        |  workgroupcategory String, workgroupcategoryname String, deptno String, deptname String,
        |  projectcode String, projectjoindate Timestamp, projectenddate Timestamp,attendance String,
        |  utilization String,salary String)
        | PARTITIONED BY (doj Timestamp, empname String)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiontwoalldims OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(sql("select count(*) from partitiontwoalldims"), Seq(Row(10)))
  }

  test("partition with different order column issue") {
    try {
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FAIL.name())

      sql("drop table if exists partdatecarb4_hive")
      sql(
        "create table partdatecarb4_hive(name string, age Int) partitioned by(country string, state string, city string)")

      sql("insert into partdatecarb4_hive partition(state,city,country='india') select 'name1',12,'KA', 'BGLR'")
      sql("insert into partdatecarb4_hive partition(state,city,country='india') select 'name1',12,'KA', 'BGLR'")

      sql("drop table if exists partdatecarb4")
      sql(
        "create table partdatecarb4(name string, age Int) partitioned by(country string, state string, city string) STORED AS carbondata")

      sql("insert into partdatecarb4 partition(state,city,country='india') select 'name1',12,'KA', 'BGLR'")
      sql("insert into partdatecarb4 partition(city,state,country='india') select 'name1',12, 'BGLR','KA'")
      sql("select * from partdatecarb4").show()
      checkAnswer(sql("select * from partdatecarb4"), sql("select * from partdatecarb4_hive"))
      intercept[Exception] {
        sql(
          "insert into partdatecarb4 partition(state,city='3',country) select 'name1',12,'cc', 'dd'")
      }
    } finally {
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FORCE.name())
    }
  }

  test("data loading for decimal column partition table") {

    sql(
      """
        | CREATE TABLE partitiondecimal (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int, doj Timestamp, empname String)
        | PARTITIONED BY (salary decimal)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiondecimal OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    checkAnswer(sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from partitiondecimal order by empno"),
      sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable order by empno"))
  }

  test("data loading for decimal column static partition table") {

    sql(
      """
        | CREATE TABLE partitiondecimalstatic (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int, doj Timestamp, empname String)
        | PARTITIONED BY (salary decimal)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiondecimalstatic partition(salary='1.0') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    checkAnswer(sql("select count(salary) from partitiondecimalstatic"), Seq(Row(10)))
  }

  test("query after select on partition table") {

    sql(
      """
        | CREATE TABLE partitiondatadelete (designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int,empno int, projectenddate Timestamp,attendance int,
        |  utilization int, doj Timestamp, empname String,salary int)
        | PARTITIONED BY (projectjoindate Timestamp)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiondatadelete OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(s"delete from partitiondatadelete where projectjoindate='2012-11-14 00:00:00'")
    checkAnswer(sql(s"select count(*) from partitiondatadelete where where projectjoindate='2012-11-14 00:00:00'"), Seq(Row(0)))
  }

  test("partition colunm test without partition column in fileheader of load command") {
    sql("DROP TABLE IF EXISTS partitiontablewithoutpartcolumninfileheader")

    sql("CREATE TABLE partitiontablewithoutpartcolumninfileheader (CUST_ID int,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) partitioned by(CUST_NAME String) STORED AS carbondata ")
    sql(s"""LOAD DATA INPATH '$resourcesPath/data_with_all_types.csv' into table partitiontablewithoutpartcolumninfileheader partition(cust_name='ravi') OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME1,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""")

    checkAnswer(sql("select count(*) from partitiontablewithoutpartcolumninfileheader"), Seq(Row(10)))
    sql("DROP TABLE IF EXISTS partitiontablewithoutpartcolumninfileheader")
  }

  test("data loading with wrong format in static partition table") {
    sql("DROP TABLE IF EXISTS partitionwrongformat")
    sql(
      """
        | CREATE TABLE partitionwrongformat (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectenddate Timestamp,attendance int,
        |  utilization int, doj Timestamp, empname String)
        | PARTITIONED BY (projectjoindate Timestamp, salary decimal)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    intercept[MalformedCarbonCommandException] {
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionwrongformat partition(projectjoindate='2016-12-01',salary='gg') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    }

    intercept[MalformedCarbonCommandException] {
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionwrongformat partition(projectjoindate='2016',salary='1.0') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    }

  }

  test("data loading with default partition in static partition table") {
    sql("DROP TABLE IF EXISTS partitiondefaultpartition")
    sql(
      """
        | CREATE TABLE partitiondefaultpartition (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectenddate Timestamp,attendance int,
        |  utilization int, doj Timestamp, empname String)
        | PARTITIONED BY (projectjoindate Timestamp, salary decimal)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiondefaultpartition partition(projectjoindate='__HIVE_DEFAULT_PARTITION__',salary='1.0') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(sql("select count(salary) from partitiondefaultpartition"), Seq(Row(10)))
    checkExistence(sql("show partitions partitiondefaultpartition"), true, "__HIVE_DEFAULT_PARTITION__")
  }

  test("data loading with default partition in static partition table with fail badrecord") {
    sql("DROP TABLE IF EXISTS partitiondefaultpartitionfail")
    sql(
      """
        | CREATE TABLE partitiondefaultpartitionfail (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectenddate Timestamp,attendance int,
        |  utilization int, doj Timestamp, empname String)
        | PARTITIONED BY (projectjoindate Timestamp, salary decimal)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiondefaultpartitionfail partition(projectjoindate='__HIVE_DEFAULT_PARTITION__',salary='1.0') OPTIONS('bad_records_logger_enable'='true', 'bad_records_action'='fail','DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(sql("select count(*) from partitiondefaultpartitionfail"), Seq(Row(10)))
    checkExistence(sql("show partitions partitiondefaultpartitionfail"), true, "__HIVE_DEFAULT_PARTITION__")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiondefaultpartitionfail partition(projectjoindate='2016-12-01',salary='__HIVE_DEFAULT_PARTITION__') OPTIONS('bad_records_logger_enable'='true', 'bad_records_action'='fail','DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(sql("select count(*) from partitiondefaultpartitionfail"), Seq(Row(20)))
  }

  test("data loading with int partition issue") {
    sql("DROP TABLE IF EXISTS intissue")
    sql("create table intissue(a int) partitioned by (b int) STORED AS carbondata")
    sql("insert into intissue values(1,1)")
    checkAnswer(sql("select * from intissue"), Seq(Row(1,1)))
  }

  test("data loading with int partition issue with global sort") {
    sql("DROP TABLE IF EXISTS intissuesort")
    sql("create table intissuesort(a int) partitioned by (b int) STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')")
    sql("insert into intissuesort values(1,1)")
    checkAnswer(sql("select * from intissuesort"), Seq(Row(1,1)))
  }

  test("data loading with decimal column fail issue") {
    sql("DROP TABLE IF EXISTS partitiondecimalfailissue")
    sql("CREATE TABLE IF NOT EXISTS partitiondecimalfailissue (ID Int, date Timestamp, country String, name String, phonetype String, serialname String) partitioned by (salary Decimal(17,2)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/decimalDataWithHeader.csv' into table partitiondecimalfailissue")
    sql(s"select * from partitiondecimalfailissue").show()
    sql(s"insert into partitiondecimalfailissue partition(salary='13000000.7878788') select ID,date,country,name,phonetype,serialname from partitiondecimalfailissue" )
    sql(s"select * from partitiondecimalfailissue").show(100)
  }

  test("data loading with decimalissue partition issue") {
    sql("DROP TABLE IF EXISTS decimalissue")
    sql("create table decimalissue(a int) partitioned by (b decimal(2,2)) STORED AS carbondata")
    sql("insert into decimalissue values(23,21.2)")
    checkAnswer(sql("select * from decimalissue"), Seq(Row(23,null)))
  }

  test("data loading scalar query partition issue") {
    sql("DROP TABLE IF EXISTS scalarissue")
    sql("create table scalarissue(a int) partitioned by (salary double) STORED AS carbondata")
    sql("insert into scalarissue values(23,21.2)")
    sql("DROP TABLE IF EXISTS scalarissue_hive")
    sql("create table scalarissue_hive(a int,salary double) using parquet partitioned by (salary) ")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sql("insert into scalarissue_hive values(23,21.2)")
    if (SparkUtil.isSparkVersionEqualTo("2.1") || SparkUtil.isSparkVersionEqualTo("2.2")) {
      intercept[Exception] {
        sql(s"select * from scalarissue_hive where salary = (select max(salary) from " +
            s"scalarissue_hive)")
          .show()
      }
      intercept[Exception] {
        sql(s"select * from scalarissue where salary = (select max(salary) from scalarissue)")
          .show()
      }
    } else {
      checkAnswer(sql(s"select * from scalarissue_hive where salary = (select max(salary) from " +
                      s"scalarissue_hive)"), Seq(Row(23, 21.2)))
      checkAnswer(sql(s"select * from scalarissue where salary = (select max(salary) from " +
                      s"scalarissue)"),
        Seq(Row(23, 21.2)))
    }
  }

  test("global sort badrecords fail on partition column message") {
    sql("DROP TABLE IF EXISTS badrecordsPartitionfailmessage")
    sql("create table badrecordsPartitionfailmessage(intField1 int, stringField1 string) partitioned by (intField2 int) STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')")
    val ex = intercept[Exception] {
      sql(s"load data local inpath '$resourcesPath/data_partition_badrecords.csv' into table badrecordsPartitionfailmessage options('bad_records_action'='fail')")
    }
    println(ex.getMessage.startsWith("DataLoad failure: Data load failed due to bad record"))
  }

  test("multiple compaction on partition table") {
    sql("DROP TABLE IF EXISTS comp_dt2")
    sql("create table comp_dt2(id int,name string) partitioned by (dt date,c4 int) STORED AS carbondata")
    sql("insert into comp_dt2 select 1,'A','2001-01-01',1")
    sql("insert into comp_dt2 select 2,'B','2001-01-01',1")
    sql("insert into comp_dt2 select 3,'C','2002-01-01',2")
    sql("insert into comp_dt2 select 4,'D','2002-01-01',null")
    assert(sql("select * from comp_dt2").collect().length == 4)
    sql("Alter table comp_dt2 compact 'minor'")
    assert(sql("select * from comp_dt2").collect().length == 4)
    sql("clean files for table comp_dt2")
    assert(sql("select * from comp_dt2").collect().length == 4)
    sql("insert into comp_dt2 select 5,'E','2003-01-01',3")
    sql("insert into comp_dt2 select 6,'F','2003-01-01',3")
    sql("insert into comp_dt2 select 7,'G','2003-01-01',4")
    sql("insert into comp_dt2 select 8,'H','2004-01-01',''")
    assert(sql("select * from comp_dt2").collect().length == 8)
    sql("Alter table comp_dt2 compact 'minor'")
    sql("clean files for table comp_dt2")
    assert(sql("select * from comp_dt2").collect().length == 8)
    assert(sql("select * from comp_dt2").collect().length == 8)
    sql("insert into comp_dt2 select 9,'H','2001-01-01',1")
    sql("insert into comp_dt2 select 10,'I','2002-01-01',null")
    sql("insert into comp_dt2 select 11,'J','2003-01-01',4")
    sql("insert into comp_dt2 select 12,'K','2003-01-01',5")
    assert(sql("select * from comp_dt2").collect().length == 12)
    sql("Alter table comp_dt2 compact 'minor'")
    assert(sql("show segments for table comp_dt2").collect().length == 8)
    assert(sql("select * from comp_dt2").collect().length == 12)
    sql("clean files for table comp_dt2")
    assert(sql("select * from comp_dt2").collect().length == 12)
    sql("insert into comp_dt2 select 13,'L','2004-01-01', 6")
    assert(sql("select * from comp_dt2").collect().length == 13)
    sql("Alter table comp_dt2 compact 'major'")
    assert(sql("select * from comp_dt2").collect().length == 13)
    assert(sql("show segments for table comp_dt2").collect().length == 3)
    assert(sql("select * from comp_dt2").collect().length == 13)
    sql("clean files for table comp_dt2")
    assert(sql("show segments for table comp_dt2").collect().length == 1)
    assert(sql("select * from comp_dt2").collect().length == 13)
  }

  test("test insert into partition column which does not exists") {
    sql("drop table if exists partitionNoColumn")
    sql("create table partitionNoColumn (name string, dob date) partitioned by(year int,month int) STORED AS carbondata")
    val exMessage = intercept[Exception] {
      sql("insert into partitionNoColumn partition(year=2014,month=01,day=01) select 'martin','2014-04-07'")
    }
    assert(exMessage.getMessage.contains("day is not a valid partition column in table default.partitionnocolumn"))
  }

  test("data loading with default partition in static partition table with local_sort") {
    sql("DROP TABLE IF EXISTS partitiondefaultlocalsort")
    sql(
      """
        | CREATE TABLE partitiondefaultlocalsort (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectenddate Timestamp,attendance int,
        |  utilization int, doj Timestamp, empname String)
        | PARTITIONED BY (projectjoindate Timestamp, salary decimal)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='local_sort')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiondefaultlocalsort OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(sql("select count(*) from partitiondefaultlocalsort"), Seq(Row(10)))
  }

  test("data loading with default partition in static partition table with nosort") {
    sql("DROP TABLE IF EXISTS partitiondefaultnosort")
    sql(
      """
        | CREATE TABLE partitiondefaultnosort (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectenddate Timestamp,attendance int,
        |  utilization int, doj Timestamp, empname String)
        | PARTITIONED BY (projectjoindate Timestamp, salary decimal)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='NO_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiondefaultnosort OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(sql("select count(*) from partitiondefaultnosort"), Seq(Row(10)))
  }

  test("data loading with default partition in static partition table with rename") {
    sql("DROP TABLE IF EXISTS partitiondefaultrename")
    sql("DROP TABLE IF EXISTS partitiondefaultrename_new")
    sql(
      """
        | CREATE TABLE partitiondefaultrename (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectenddate Timestamp,attendance int,
        |  utilization int, doj Timestamp, empname String)
        | PARTITIONED BY (projectjoindate Timestamp, salary decimal)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiondefaultrename OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(sql("select count(*) from partitiondefaultrename"), Seq(Row(10)))
    sql(s"alter table partitiondefaultrename rename to partitiondefaultrename_new")
    checkAnswer(sql("select count(*) from partitiondefaultrename_new"), Seq(Row(10)))
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiondefaultrename_new OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(sql("select count(*) from partitiondefaultrename_new"), Seq(Row(20)))
  }

  test("data loading with default partition in static partition table with rename first") {
    sql("DROP TABLE IF EXISTS partitiondefaultrenamefirst")
    sql("DROP TABLE IF EXISTS partitiondefaultrenamefirst_new")
    sql(
      """
        | CREATE TABLE partitiondefaultrenamefirst (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectenddate Timestamp,attendance int,
        |  utilization int, doj Timestamp, empname String)
        | PARTITIONED BY (projectjoindate Timestamp, salary decimal)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"alter table partitiondefaultrenamefirst rename to partitiondefaultrenamefirst_new")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiondefaultrenamefirst_new OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(sql("select count(*) from partitiondefaultrenamefirst_new"), Seq(Row(10)))
  }

  test("data loading for global partition table for two partition column with no columns in csv") {
    sql("DROP TABLE IF EXISTS partitiontwonocolumns")
    sql(
      """
        | CREATE TABLE partitiontwonocolumns (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int,doj Timestamp, empname String)
        | PARTITIONED BY (newcol1 date, newcol2 int)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiontwonocolumns partition(newcol1='2016-08-09', newcol2='20') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    checkAnswer(sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from partitiontwonocolumns order by empno"),
      sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable order by empno"))

    checkAnswer(sql("select distinct cast(newcol1 as string) from partitiontwonocolumns"), Seq(Row("2016-08-09")))
  }

  override def afterAll = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_TASK_DISTRIBUTION,
        CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_DEFAULT)
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION , LoggerAction.FORCE.name())
    dropTable
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
    sql("drop table if exists partitiondecimal")
    sql("drop table if exists partitiondecimalstatic")
    sql("drop table if exists partitiondatadelete")
    sql("drop table if exists comp_dt2")
    sql("drop table if exists partitionNoColumn")
    sql("drop table if exists partitiondefaultlocalsort")
  }
}

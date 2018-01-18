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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class StandardPartitionTableOverwriteTestCase extends QueryTest with BeforeAndAfterAll {

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
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE originTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

  }

  test("overwriting static partition table for date partition column on insert query") {
    sql(
      """
        | CREATE TABLE staticpartitiondateinsert (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp,attendance int,
        |  deptname String,projectcode int,
        |  utilization int,salary int)
        | PARTITIONED BY (projectenddate Date,doj Timestamp)
        | STORED BY 'org.apache.carbondata.format'
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

  test("overwriting partition table for date partition column on insert query") {
    sql(
      """
        | CREATE TABLE partitiondateinsert (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp,attendance int,
        |  deptname String,projectcode int,
        |  utilization int,salary int)
        | PARTITIONED BY (projectenddate Date,doj Timestamp)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"""insert into partitiondateinsert select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable""")
    sql(s"""insert into partitiondateinsert select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable""")
    sql(s"""insert into partitiondateinsert select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable""")
    sql(s"""insert into partitiondateinsert select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable""")
    sql(s"""insert overwrite table partitiondateinsert select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable where projectenddate=cast('2016-06-29' as Date)""")
    checkAnswer(sql("select * from partitiondateinsert"),
      sql("select empno, empname,designation,workgroupcategory,workgroupcategoryname,deptno,projectjoindate,attendance,deptname,projectcode,utilization,salary,projectenddate,doj from originTable where projectenddate=cast('2016-06-29' as Date)"))
  }

  test("dynamic and static partition table with load syntax") {
    sql(
      """
        | CREATE TABLE loadstaticpartitiondynamic (designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int, empname String)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE loadstaticpartitiondynamic PARTITION(empno='1', empname) OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(sql(s"select count(*) from loadstaticpartitiondynamic where empno=1"), sql(s"select count(*) from loadstaticpartitiondynamic"))
  }

  test("dynamic and static partition table with overwrite ") {
    sql(
      """
        | CREATE TABLE insertstaticpartitiondynamic (designation String, doj Timestamp,salary int)
        | PARTITIONED BY (empno int, empname String)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE insertstaticpartitiondynamic PARTITION(empno, empname) OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    val rows = sql(s"select count(*) from insertstaticpartitiondynamic").collect()
    sql("""insert overwrite table insertstaticpartitiondynamic PARTITION(empno='1', empname) select designation, doj, salary, empname from insertstaticpartitiondynamic""")

    checkAnswer(sql(s"select count(*) from insertstaticpartitiondynamic where empno=1"), rows)

    intercept[Exception] {
      sql("""insert overwrite table insertstaticpartitiondynamic PARTITION(empno, empname='ravi') select designation, doj, salary, empname from insertstaticpartitiondynamic""")
    }

  }

  test("overwriting all partition on table and do compaction") {
    sql(
      """
        | CREATE TABLE partitionallcompaction (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (deptname String,doj Timestamp,projectcode int)
        | STORED BY 'org.apache.carbondata.format'
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

  test("Test overwrite static partition ") {
    sql(
      """
        | CREATE TABLE weather6 (type String)
        | PARTITIONED BY (year int, month int, day int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql("insert into weather6 partition(year=2014, month=5, day=25) select 'rainy'")
    sql("insert into weather6 partition(year=2014, month=4, day=23) select 'cloudy'")
    sql("insert overwrite table weather6 partition(year=2014, month=5, day=25) select 'sunny'")
    checkExistence(sql("select * from weather6"), true, "sunny")
    checkAnswer(sql("select count(*) from weather6"), Seq(Row(2)))
  }

  test("Test overwrite static partition with wrong int value") {
    sql(
      """
        | CREATE TABLE weather7 (type String)
        | PARTITIONED BY (year int, month int, day int)
        | STORED BY 'org.apache.carbondata.format'
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


  override def afterAll = {
//    dropTable
  }

  def dropTable = {
    sql("drop table if exists originTable")
    sql("drop table if exists partitiondateinsert")
    sql("drop table if exists staticpartitiondateinsert")
    sql("drop table if exists loadstaticpartitiondynamic")
    sql("drop table if exists insertstaticpartitiondynamic")
    sql("drop table if exists partitionallcompaction")
    sql("drop table if exists weather6")
    sql("drop table if exists weather7")
  }

}

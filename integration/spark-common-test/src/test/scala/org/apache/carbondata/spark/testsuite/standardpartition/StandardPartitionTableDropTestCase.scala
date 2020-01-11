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

import java.nio.file.{Files, LinkOption, Paths}

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.TestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class StandardPartitionTableDropTestCase extends QueryTest with BeforeAndAfterAll {

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

  test("show partitions on partition table") {
    sql(
      """
        | CREATE TABLE partitionshow (designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int, empname String)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionshow OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkExistence(sql(s"""SHOW PARTITIONS partitionshow"""), true, "empno=11", "empno=12")
  }

  test("droping on partition table for int partition column") {
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
    checkAnswer(
      sql(s"""select count (*) from partitionone"""),
      sql(s"""select count (*) from originTable"""))

    checkAnswer(
      sql(s"""select count (*) from partitionone where empno=11"""),
      sql(s"""select count (*) from originTable where empno=11"""))

    sql(s"""ALTER TABLE partitionone DROP PARTITION(empno='11')""")

    checkExistence(sql(s"""SHOW PARTITIONS partitionone"""), false, "empno=11")

    checkAnswer(
      sql(s"""select count (*) from partitionone where empno=11"""),
      Seq(Row(0)))
  }

    test("dropping partition on table for more partition columns") {
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
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionmany OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
      sql(s"""ALTER TABLE partitionmany DROP PARTITION(deptname='Learning')""")
      checkExistence(sql(s"""SHOW PARTITIONS partitionmany"""), false, "deptname=Learning", "projectcode=928479")
      checkAnswer(
        sql(s"""select count (*) from partitionmany where deptname='Learning'"""),
        Seq(Row(0)))
    }

  test("dropping all partition on table") {
    sql(
      """
        | CREATE TABLE partitionall (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (deptname String,doj Timestamp,projectcode int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionall OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionall OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""ALTER TABLE partitionall DROP PARTITION(deptname='Learning')""")
    sql(s"""ALTER TABLE partitionall DROP PARTITION(deptname='configManagement')""")
    sql(s"""ALTER TABLE partitionall DROP PARTITION(deptname='network')""")
    sql(s"""ALTER TABLE partitionall DROP PARTITION(deptname='protocol')""")
    sql(s"""ALTER TABLE partitionall DROP PARTITION(deptname='security')""")
    assert(sql(s"""SHOW PARTITIONS partitionall""").collect().length == 0)
    checkAnswer(
      sql(s"""select count (*) from partitionall"""),
      Seq(Row(0)))
  }

  test("dropping static partition on table") {
    sql(
      """
        | CREATE TABLE staticpartition (empno int, doj Timestamp,
        |  workgroupcategoryname String, deptno int,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int,workgroupcategory int, empname String, designation String)
        | PARTITIONED BY (deptname String)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""insert into staticpartition PARTITION(deptname='software') select empno,doj,workgroupcategoryname,deptno,projectcode,projectjoindate,projectenddate,attendance,utilization,salary,workgroupcategory,empname,designation from originTable""")

    checkExistence(sql(s"""SHOW PARTITIONS staticpartition"""), true, "deptname=software")
    assert(sql(s"""SHOW PARTITIONS staticpartition""").collect().length == 1)
    sql(s"""ALTER TABLE staticpartition DROP PARTITION(deptname='software')""")
    checkAnswer(
      sql(s"""select count (*) from staticpartition"""),
      Seq(Row(0)))
    sql(s"""insert into staticpartition select empno,doj,workgroupcategoryname,deptno,projectcode,projectjoindate,projectenddate,attendance,utilization,salary,workgroupcategory,empname,designation,deptname from originTable""")
    checkExistence(sql(s"""SHOW PARTITIONS staticpartition"""), true, "deptname=protocol")
    checkAnswer(
      sql(s"""select count (*) from staticpartition"""),
      sql(s"""select count (*) from originTable"""))

  }


  test("dropping all partition on table and do compaction") {
    sql(
      """
        | CREATE TABLE partitionallcompaction (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (deptname String,doj Timestamp,projectcode int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionallcompaction OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionallcompaction OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionallcompaction OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionallcompaction OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""ALTER TABLE partitionallcompaction DROP PARTITION(deptname='Learning')""")
    sql(s"""ALTER TABLE partitionallcompaction DROP PARTITION(deptname='configManagement')""")
    sql(s"""ALTER TABLE partitionallcompaction DROP PARTITION(deptname='network')""")
    sql(s"""ALTER TABLE partitionallcompaction DROP PARTITION(deptname='protocol')""")
    sql(s"""ALTER TABLE partitionallcompaction DROP PARTITION(deptname='security')""")
    assert(sql(s"""SHOW PARTITIONS partitionallcompaction""").collect().length == 0)
    sql("ALTER TABLE partitionallcompaction COMPACT 'MAJOR'").collect()
    checkAnswer(
      sql(s"""select count (*) from partitionallcompaction"""),
      Seq(Row(0)))
  }

  test("test dropping on partition table for int partition column") {
    sql(
      """
        | CREATE TABLE partitionone1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionone1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(
      sql(s"""select count (*) from partitionone1"""),
      sql(s"""select count (*) from originTable"""))

    checkAnswer(
      sql(s"""select count (*) from partitionone1 where empno=11"""),
      sql(s"""select count (*) from originTable where empno=11"""))
    sql(s"""ALTER TABLE partitionone1 DROP PARTITION(empno='11')""")
    sql(s"CLEAN FILES FOR TABLE partitionone1").show()
    assert(Files.notExists(Paths.get(TestQueryExecutor.warehouse + "/partitionone1/" + "empno=11"), LinkOption.NOFOLLOW_LINKS))
    sql("drop table if exists partitionone1")
  }

  override def afterAll = {
    dropTable
  }

  def dropTable = {
    sql("drop table if exists originTable")
    sql("drop table if exists originMultiLoads")
    sql("drop table if exists partitionone")
    sql("drop table if exists partitionall")
    sql("drop table if exists partitionmany")
    sql("drop table if exists partitionshow")
    sql("drop table if exists staticpartition")
    sql("drop table if exists partitionallcompaction")
    sql("drop table if exists partitionone1")
  }

}

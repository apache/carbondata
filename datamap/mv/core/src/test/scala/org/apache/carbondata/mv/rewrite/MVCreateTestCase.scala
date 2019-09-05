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
package org.apache.carbondata.mv.rewrite

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

class MVCreateTestCase extends CarbonQueryTest with BeforeAndAfterAll {

  override def beforeAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    val projectPath = new File(this.getClass.getResource("/").getPath + "../../../../../")
      .getCanonicalPath.replaceAll("\\\\", "/")
    val integrationPath = s"$projectPath/integration"
    val resourcesPath = s"$integrationPath/spark-common-test/src/test/resources"
    sql(
      """
        | CREATE TABLE fact_table1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(
      """
        | CREATE TABLE fact_table2 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(
      """
        | CREATE TABLE fact_table3 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table3 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table3 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(
      """
        | CREATE TABLE fact_table4 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table4 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table4 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(
      """
        | CREATE TABLE fact_table5 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(
      """
        | CREATE TABLE fact_table6 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table6 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table6 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
  }

  test("test create datamap with simple and same projection") {
    sql("drop datamap if exists datamap1")
    sql("create datamap datamap1 using 'mv' as select empname, designation from fact_table1")
    val df = sql("select empname,designation from fact_table1")
    val analyzed = df.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap1"))
    checkAnswer(df, sql("select empname,designation from fact_table2"))
    sql(s"drop datamap datamap1")
  }

  test("test create datamap with simple and sub projection") {
    sql("drop datamap if exists datamap2")
    sql("create datamap datamap2 using 'mv' as select empname, designation from fact_table1")
    val df = sql("select empname from fact_table1")
    val analyzed = df.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap2"))
    checkAnswer(df, sql("select empname from fact_table2"))
    sql(s"drop datamap datamap2")
  }

  test("test create datamap with simple and same projection with projection filter") {
    sql("drop datamap if exists datamap3")
    sql("create datamap datamap3 using 'mv' as select empname, designation from fact_table1")
    val frame = sql("select empname, designation from fact_table1 where empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap3"))

    checkAnswer(frame, sql("select empname, designation from fact_table2 where empname='shivani'"))
    sql(s"drop datamap datamap3")
  }

  test("test create datamap with simple and sub projection with non projection filter") {
    sql("create datamap datamap4 using 'mv' as select empname, designation from fact_table1")
    val frame = sql("select designation from fact_table1 where empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap4"))
    checkAnswer(frame, sql("select designation from fact_table2 where empname='shivani'"))
    sql(s"drop datamap datamap4")
  }

  test("test create datamap with simple and sub projection with datamap filter") {
    sql("create datamap datamap5 using 'mv' as select empname, designation from fact_table1 where empname='shivani'")
    val frame = sql("select designation from fact_table1 where empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap5"))
    checkAnswer(frame, sql("select designation from fact_table2 where empname='shivani'"))
    sql(s"drop datamap datamap5")
  }

  test("test create datamap with simple and same projection with datamap filter ") {
    sql("create datamap datamap6 using 'mv' as select empname, designation from fact_table1 where empname='shivani'")
    val frame = sql("select empname,designation from fact_table1 where empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap6"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where empname='shivani'"))
    sql(s"drop datamap datamap6")
  }

  test("test create datamap with simple and same projection with datamap filter and extra query column filter") {
    sql("create datamap datamap7 using 'mv' as select empname, designation from fact_table1 where empname='shivani'")
    val frame = sql(
      "select empname,designation from fact_table1 where empname='shivani' and designation='SA'")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap7"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where empname='shivani' and designation='SA'"))
    sql(s"drop datamap datamap7")
  }

  test("test create datamap with simple and same projection with datamap filter and different column filter") {
    sql("create datamap datamap8 using 'mv' as select empname, designation from fact_table1 where empname='shivani'")
    val frame = sql("select empname,designation from fact_table1 where designation='SA'")
    val analyzed = frame.queryExecution.analyzed
    assert(!TestUtil.verifyMVDataMap(analyzed, "datamap8"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where designation='SA'"))
    sql(s"drop datamap datamap8")
  }

  test("test create datamap with simple and same projection with datamap filter on non projection column and extra column filter") {
    sql("create datamap datamap9 using 'mv' as select empname, designation,deptname  from fact_table1 where deptname='cloud'")
    val frame = sql("select empname,designation from fact_table1 where deptname='cloud'")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap9"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where deptname='cloud'"))
    sql(s"drop datamap datamap9")
  }

  test("test create datamap with simple and same projection with datamap filter on non projection column and no column filter") {
    sql("create datamap datamap10 using 'mv' as select empname, designation,deptname from fact_table1 where deptname='cloud'")
    val frame = sql("select empname,designation from fact_table1")
    val analyzed = frame.queryExecution.analyzed
    assert(!TestUtil.verifyMVDataMap(analyzed, "datamap10"))
    checkAnswer(frame, sql("select empname,designation from fact_table2"))
    sql(s"drop datamap datamap10")
  }

  test("test create datamap with simple and same projection with datamap filter on non projection column and different column filter") {
    sql("create datamap datamap11 using 'mv' as select empname, designation,deptname from fact_table1 where deptname='cloud'")
    val frame = sql("select empname,designation from fact_table1 where designation='SA'")
    val analyzed = frame.queryExecution.analyzed
    assert(!TestUtil.verifyMVDataMap(analyzed, "datamap11"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where designation='SA'"))
    sql(s"drop datamap datamap11")
  }

  test("test create datamap with simple and same group by query") {
    sql("drop datamap if exists datamap12")
    sql("create datamap datamap12 using 'mv' as select empname, sum(utilization) from fact_table1 group by empname")
    val frame = sql("select empname, sum(utilization) from fact_table1 group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap12"))
    checkAnswer(frame, sql("select empname, sum(utilization) from fact_table2 group by empname"))
    sql(s"drop datamap datamap12")
  }

  test("test create datamap with simple and sub group by query") {
    sql("drop datamap if exists datamap13")
    sql("create datamap datamap13 using 'mv' as select empname, sum(utilization) from fact_table1 group by empname")
    val frame = sql("select sum(utilization) from fact_table1 group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap13"))
    checkAnswer(frame, sql("select sum(utilization) from fact_table2 group by empname"))
    sql(s"drop datamap datamap13")
  }

  test("test create datamap with simple and sub group by query with filter on query") {
    sql("drop datamap if exists datamap14")
    sql("create datamap datamap14 using 'mv' as select empname, sum(utilization) from fact_table1 group by empname")
    val frame = sql(
      "select empname,sum(utilization) from fact_table1 group by empname having empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap14"))
    checkAnswer(frame, sql("select empname,sum(utilization) from fact_table2 where empname='shivani' group by empname"))
    sql(s"drop datamap datamap14")
  }

  test("test create datamap with simple and sub group and sub projection by query with filter on query") {
    sql("drop datamap if exists datamap32")
    sql("create datamap datamap32 using 'mv' as select empname, sum(utilization) from fact_table1 group by empname")
    val frame = sql(
      "select empname, sum(utilization) from fact_table1 group by empname having empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap32"))
    checkAnswer(frame, sql( "select empname, sum(utilization) from fact_table2 group by empname having empname='shivani'"))
    sql(s"drop datamap datamap32")
  }

  test("test create datamap with simple and sub group by query with filter on datamap") {
    sql("create datamap datamap15 using 'mv' as select empname, sum(utilization) from fact_table1 where empname='shivani' group by empname")
    val frame = sql(
      "select empname,sum(utilization) from fact_table1 where empname='shivani' group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap15"))
    checkAnswer(frame, sql("select empname,sum(utilization) from fact_table2 where empname='shivani' group by empname"))
    sql(s"drop datamap datamap15")
  }

  test("test create datamap with simple and sub group by query with filter on datamap and no filter on query") {
    sql("create datamap datamap16 using 'mv' as select empname, sum(utilization) from fact_table1 where empname='shivani' group by empname")
    val frame = sql("select empname,sum(utilization) from fact_table1 group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(!TestUtil.verifyMVDataMap(analyzed, "datamap16"))
    checkAnswer(frame, sql("select empname,sum(utilization) from fact_table2 group by empname"))
    sql(s"drop datamap datamap16")
  }

  test("test create datamap with simple and same group by with expression") {
    sql("create datamap datamap17 using 'mv' as select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    val frame = sql(
      "select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group" +
      " by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap17"))
    checkAnswer(frame, sql("select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table2 group" +
                           " by empname"))
    sql(s"drop datamap datamap17")
  }

  test("test create datamap with simple and sub group by with expression") {
    sql("drop datamap if exists datamap18")
    sql("create datamap datamap18 using 'mv' as select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    val frame = sql(
      "select sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap18"))
    checkAnswer(frame, sql("select sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table2 group by empname"))
    sql(s"drop datamap datamap18")
  }

  test("test create datamap with simple and sub count group by with expression") {
    sql("drop datamap if exists datamap19")
    sql("create datamap datamap19 using 'mv' as select empname, count(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    val frame = sql(
      "select count(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap19"))
    checkAnswer(frame, sql("select count(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table2 group by empname"))
    sql(s"drop datamap datamap19")
  }

  test("test create datamap with simple and sub group by with expression and filter on query") {
    sql("drop datamap if exists datamap20")
    sql("create datamap datamap20 using 'mv' as select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    val frame = sql(
      "select sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 where " +
      "empname='shivani' group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap20"))
    checkAnswer(frame, sql("select sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table2 where " +
                           "empname='shivani' group by empname"))
    sql(s"drop datamap datamap20")
  }

  test("test create datamap with simple join") {
    sql("drop datamap if exists datamap21")
    sql("create datamap datamap21 using 'mv' as select t1.empname as c1, t2.designation, t2.empname as c2 from fact_table1 t1 inner join fact_table2 t2  on (t1.empname = t2.empname)")
    val frame = sql(
      "select t1.empname as c1, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = t2.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap21"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = t2.empname"))
    sql(s"drop datamap datamap21")
  }

  test("test create datamap with simple join and filter on query") {
    sql("drop datamap if exists datamap22")
    sql("create datamap datamap22 using 'mv' as select t1.empname, t2.designation,t2.empname from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname)")
    val frame = sql(
      "select t1.empname, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = " +
      "t2.empname and t1.empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap22"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = " +
                           "t2.empname and t1.empname='shivani'"))
    sql(s"drop datamap datamap22")
  }


  test("test create datamap with simple join and filter on query and datamap") {
    sql("drop datamap if exists datamap23")
    sql("create datamap datamap23 using 'mv' as select t1.empname, t2.designation, t2.empname from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname) where t1.empname='shivani'")
    val frame = sql(
      "select t1.empname, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = " +
      "t2.empname and t1.empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap23"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = " +
                           "t2.empname and t1.empname='shivani'"))
    sql(s"drop datamap datamap23")
  }

  test("test create datamap with simple join and filter on datamap and no filter on query") {
    sql("drop datamap if exists datamap24")
    sql("create datamap datamap24 using 'mv' as select t1.empname, t2.designation, t2.empname from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname) where t1.empname='shivani'")
    val frame = sql(
      "select t1.empname, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = t2.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(!TestUtil.verifyMVDataMap(analyzed, "datamap24"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = t2.empname"))
    sql(s"drop datamap datamap24")
  }

  test("test create datamap with multiple join") {
    sql("drop datamap if exists datamap25")
    sql("create datamap datamap25 using 'mv' as select t1.empname as c1, t2.designation, t2.empname, t3.empname from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname) inner join fact_table3 t3  on (t1.empname=t3.empname)")
    val frame = sql(
      "select t1.empname as c1, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = t2.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(!TestUtil.verifyMVDataMap(analyzed, "datamap25"))
    val frame1 = sql(
      "select t1.empname as c1, t2.designation from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname) inner join fact_table3 t3  on (t1.empname=t3.empname)")
    val analyzed1 = frame1.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed1, "datamap25"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = t2.empname"))
    sql(s"drop datamap datamap25")
  }

  ignore("test create datamap with simple join on datamap and multi join on query") {
    sql("create datamap datamap26 using 'mv' as select t1.empname, t2.designation, t2.empname from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname)")
    val frame = sql(
      "select t1.empname, t2.designation from fact_table1 t1,fact_table2 t2,fact_table3 " +
      "t3  where t1.empname = t2.empname and t1.empname=t3.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap26"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2,fact_table6 " +
                           "t3  where t1.empname = t2.empname and t1.empname=t3.empname"))
    sql(s"drop datamap datamap26")
  }

  test("test create datamap with join with group by") {
    sql("create datamap datamap27 using 'mv' as select  t1.empname , t2.designation, sum(t1.utilization), sum(t2.empname) from fact_table1 t1 inner join fact_table2 t2  on (t1.empname = t2.empname) group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname group by t1.empname, t2.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap27"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  " +
                           "where t1.empname = t2.empname group by t1.empname, t2.designation"))
    sql(s"drop datamap datamap27")
  }

  test("test create datamap with join with group by and sub projection") {
    sql("drop datamap if exists datamap28")
    sql("create datamap datamap28 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization),sum(t2.empname) from fact_table1 t1 inner join fact_table2 t2  on (t1.empname = t2.empname) group by t1.empname, t2.designation")
    val frame = sql(
      "select t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where " +
      "t1.empname = t2.empname group by t2.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap28"))
    checkAnswer(frame, sql("select t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  where " +
                           "t1.empname = t2.empname group by t2.designation"))
    sql(s"drop datamap datamap28")
  }

  test("test create datamap with join with group by and sub projection with filter") {
    sql("drop datamap if exists datamap29")
    sql("create datamap datamap29 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization),sum(t2.empname) from fact_table1 t1 inner join fact_table2 t2  on (t1.empname = t2.empname) group by t1.empname, t2.designation")
    val frame = sql(
      "select t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where " +
      "t1.empname = t2.empname and t1.empname='shivani' group by t2.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap29"))
    checkAnswer(frame, sql("select t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  where " +
                           "t1.empname = t2.empname and t1.empname='shivani' group by t2.designation"))
    sql(s"drop datamap datamap29")
  }

  test("test create datamap with join with group by and projection with filter") {
    sql("drop datamap if exists datamap29")
    sql("create datamap datamap29 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization),sum(t2.empname) from fact_table1 t1 inner join fact_table2 t2  on (t1.empname = t2.empname) group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname ,t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where " +
      "t1.empname = t2.empname and t1.empname='shivani' group by t2.designation,t1.empname ")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap29"))
    checkAnswer(frame, sql("select t1.empname ,t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  where " +
                           "t1.empname = t2.empname and t1.empname='shivani' group by t2.designation,t1.empname "))
    sql(s"drop datamap datamap29")
  }

  test("test create datamap with join with group by and sub projection with filter with alias") {
    sql("drop datamap if exists datamap29")
    sql("create datamap datamap29 using 'mv' as select t1.empname as a, t2.designation as b, sum(t1.utilization),sum(t2.empname) from fact_table1 t1 inner join fact_table2 t2  on (t1.empname = t2.empname) group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname ,t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where " +
      "t1.empname = t2.empname and t1.empname='shivani' group by t2.designation,t1.empname ")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap29"))
    checkAnswer(frame, sql("select t1.empname ,t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  where " +
                           "t1.empname = t2.empname and t1.empname='shivani' group by t2.designation,t1.empname "))
    sql(s"drop datamap datamap29")
  }

  ignore("test create datamap with join with group by with filter") {
    sql("drop datamap if exists datamap30")
    sql("create datamap datamap30 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization),sum(t2.empname) from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname) group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname and t2.designation='SA' group by t1.empname, t2.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap30"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  " +
                           "where t1.empname = t2.empname and t2.designation='SA' group by t1.empname, t2.designation"))
    sql(s"drop datamap datamap30")
  }

  ignore("test create datamap with expression on projection") {
    sql(s"drop datamap if exists datamap31")
    sql("create datamap datamap31 using 'mv' as select empname, designation, utilization, projectcode from fact_table1 ")
    val frame = sql(
      "select empname, designation, utilization+projectcode from fact_table1")
    val analyzed = frame.queryExecution.analyzed
    assert(!TestUtil.verifyMVDataMap(analyzed, "datamap31"))
    checkAnswer(frame, sql("select empname, designation, utilization+projectcode from fact_table2"))
    sql(s"drop datamap datamap31")
  }

  test("test create datamap with simple and sub group by query and count agg") {
    sql(s"drop datamap if exists datamap32")
    sql("create datamap datamap32 using 'mv' as select empname, count(utilization) from fact_table1 group by empname")
    val frame = sql("select empname,count(utilization) from fact_table1 where empname='shivani' group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap32"))
    checkAnswer(frame, sql("select empname,count(utilization) from fact_table2 where empname='shivani' group by empname"))
    sql(s"drop datamap datamap32")
  }

  ignore("test create datamap with simple and sub group by query and avg agg") {
    sql(s"drop datamap if exists datamap33")
    sql("create datamap datamap33 using 'mv' as select empname, avg(utilization) from fact_table1 group by empname")
    val frame = sql("select empname,avg(utilization) from fact_table1 where empname='shivani' group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap33"))
    checkAnswer(frame, sql("select empname,avg(utilization) from fact_table2 where empname='shivani' group by empname"))
    sql(s"drop datamap datamap33")
  }

  ignore("test create datamap with left join with group by") {
    sql("drop datamap if exists datamap34")
    sql("create datamap datamap34 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname group by t1.empname, t2.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap34"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname group by t1.empname, t2.designation"))
    sql(s"drop datamap datamap34")
  }

  ignore("test create datamap with simple and group by query with filter on datamap but not on projection") {
    sql("create datamap datamap35 using 'mv' as select designation, sum(utilization) from fact_table1 where empname='shivani' group by designation")
    val frame = sql(
      "select designation, sum(utilization) from fact_table1 where empname='shivani' group by designation")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap35"))
    checkAnswer(frame, sql("select designation, sum(utilization) from fact_table2 where empname='shivani' group by designation"))
    sql(s"drop datamap datamap35")
  }

  ignore("test create datamap with simple and sub group by query with filter on datamap but not on projection") {
    sql("create datamap datamap36 using 'mv' as select designation, sum(utilization) from fact_table1 where empname='shivani' group by designation")
    val frame = sql(
      "select sum(utilization) from fact_table1 where empname='shivani' group by designation")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap36"))
    checkAnswer(frame, sql("select sum(utilization) from fact_table2 where empname='shivani' group by designation"))
    sql(s"drop datamap datamap36")
  }

  test("test create datamap with agg push join with sub group by ") {
    sql("drop datamap if exists datamap37")
    sql("create datamap datamap37 using 'mv' as select empname, designation, sum(utilization) from fact_table1 group by empname, designation")
    val frame = sql(
      "select t1.empname, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname group by t1.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap37"))
    checkAnswer(frame, sql("select t1.empname, sum(t1.utilization) from fact_table3 t1,fact_table4 t2  " +
                           "where t1.empname = t2.empname group by t1.empname, t1.designation"))
    sql(s"drop datamap datamap37")
  }

  test("test create datamap with agg push join with group by ") {
    sql("drop datamap if exists datamap38")
    sql("create datamap datamap38 using 'mv' as select empname, designation, sum(utilization) from fact_table1 group by empname, designation")
    val frame = sql(
      "select t1.empname, t1.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname group by t1.empname,t1.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap38"))
    checkAnswer(frame, sql("select t1.empname,t1.designation, sum(t1.utilization) from fact_table3 t1,fact_table4 t2  " +
                           "where t1.empname = t2.empname group by t1.empname, t1.designation"))
    sql(s"drop datamap datamap38")
  }

  ignore("test create datamap with agg push join with group by with filter") {
    sql("drop datamap if exists datamap39")
    sql("create datamap datamap39 using 'mv' as select empname, designation, sum(utilization) from fact_table1 group by empname, designation ")
    val frame = sql(
      "select t1.empname, t1.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname and t1.empname='shivani' group by t1.empname,t1.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap39"))
    checkAnswer(frame, sql("select t1.empname,t1.designation, sum(t1.utilization) from fact_table3 t1,fact_table4 t2  " +
                           "where t1.empname = t2.empname and t1.empname='shivani' group by t1.empname, t1.designation"))
    sql(s"drop datamap datamap39")
  }

  test("test create datamap with more agg push join with group by with filter") {
    sql("drop datamap if exists datamap40")
    sql("create datamap datamap40 using 'mv' as select empname, designation, sum(utilization), count(utilization) from fact_table1 group by empname, designation ")
    val frame = sql(
      "select t1.empname, t1.designation, sum(t1.utilization),count(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname and t1.empname='shivani' group by t1.empname,t1.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap40"))
    checkAnswer(frame, sql("select t1.empname, t1.designation, sum(t1.utilization),count(t1.utilization) from fact_table3 t1,fact_table4 t2  " +
                           "where t1.empname = t2.empname and t1.empname='shivani' group by t1.empname,t1.designation"))
    sql(s"drop datamap datamap40")
  }

  ignore("test create datamap with left join with group by with filter") {
    sql("drop datamap if exists datamap41")
    sql("create datamap datamap41 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname, t2.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap41"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname, t2.designation"))
    sql(s"drop datamap datamap41")
  }

  ignore("test create datamap with left join with sub group by") {
    sql("drop datamap if exists datamap42")
    sql("create datamap datamap42 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname group by t1.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap42"))
    checkAnswer(frame, sql("select t1.empname, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname group by t1.empname"))
    sql(s"drop datamap datamap42")
  }

  ignore("test create datamap with left join with sub group by with filter") {
    sql("drop datamap if exists datamap43")
    sql("create datamap datamap43 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap43"))
    checkAnswer(frame, sql("select t1.empname, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname"))
    sql(s"drop datamap datamap43")
  }

  ignore("test create datamap with left join with sub group by with filter on mv") {
    sql("drop datamap if exists datamap44")
    sql("create datamap datamap44 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap44"))
    checkAnswer(frame, sql("select t1.empname, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname"))
    sql(s"drop datamap datamap44")
  }

  test("test create datamap with left join on query and equi join on mv with group by with filter") {
    sql("drop datamap if exists datamap45")

    sql("create datamap datamap45 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization),sum(t2.empname) from fact_table1 t1 join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    // During spark optimizer it converts the left outer join queries with equi join if any filter present on right side table
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname where t2.designation='SA' group by t1.empname, t2.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap45"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname where t2.designation='SA' group by t1.empname, t2.designation"))
    sql(s"drop datamap datamap45")
  }

  test("jira carbondata-2523") {

    sql("drop datamap if exists mv13")
    sql("drop table if exists test4")
    sql("create table test4 ( name string,age int,salary int) stored by 'carbondata'")

    sql(" insert into test4 select 'babu',12,12").show()
    sql("create datamap mv13 using 'mv' as select name,sum(salary) from test4 group by name")
    val frame = sql(
      "select name,sum(salary) from test4 group by name")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "mv13"))
  }

  test("jira carbondata-2528-1") {

    sql("drop datamap if exists MV_order")
    sql("create datamap MV_order using 'mv' as select empname,sum(salary) as total from fact_table1 group by empname")
    val frame = sql(
      "select empname,sum(salary) as total from fact_table1 group by empname order by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "MV_order"))
  }

  test("jira carbondata-2528-2") {
    sql("drop datamap if exists MV_order")
    sql("drop datamap if exists MV_desc_order")
    sql("create datamap MV_order using 'mv' as select empname,sum(salary)+sum(utilization) as total from fact_table1 group by empname")
    val frame = sql(
      "select empname,sum(salary)+sum(utilization) as total from fact_table1 group by empname order by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "MV_order"))
  }

  test("jira carbondata-2528-3") {

    sql("drop datamap if exists MV_order")
    sql("create datamap MV_order using 'mv' as select empname,sum(salary)+sum(utilization) as total from fact_table1 group by empname order by empname DESC")
    val frame = sql(
      "select empname,sum(salary)+sum(utilization) as total from fact_table1 group by empname order by empname DESC")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "MV_order"))
    sql("drop datamap if exists MV_order")
  }

  test("jira carbondata-2528-4") {

    sql("drop datamap if exists MV_order")
    sql("create datamap MV_order using 'mv' as select empname,sum(salary)+sum(utilization) as total from fact_table1 group by empname order by empname DESC")
    val frame = sql(
      "select empname,sum(salary)+sum(utilization) as total from fact_table1 where empname = 'ravi' group by empname order by empname DESC")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "MV_order"))
    sql("drop datamap if exists MV_order")
  }

  test("jira carbondata-2530") {

    sql("drop table if exists test1")
    sql("drop datamap if exists datamv2")
    sql("create table test1( name string,country string,age int,salary int) stored by 'carbondata'")
    sql("insert into test1 select 'name1','USA',12,23")
    sql("create datamap datamv2 using 'mv' as select country,sum(salary) from test1 group by country")
    val frame = sql("select country,sum(salary) from test1 where country='USA' group by country")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamv2"))
    sql("insert into test1 select 'name1','USA',12,23")
    val frame1 = sql("select country,sum(salary) from test1 where country='USA' group by country")
    val analyzed1 = frame1.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed1, "datamv2"))
    sql("drop datamap if exists datamv2")
    sql("drop table if exists test1")
  }

  test("jira carbondata-2534") {

    sql("drop datamap if exists MV_exp")
    sql("create datamap MV_exp using 'mv' as select sum(salary),substring(empname,2,5),designation from fact_table1 group by substring(empname,2,5),designation")
    val frame = sql(
      "select sum(salary),substring(empname,2,5),designation from fact_table1 group by substring(empname,2,5),designation")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "MV_exp"))
    sql("drop datamap if exists MV_exp")
  }

  test("jira carbondata-2542") {
    sql("""drop database if exists xy cascade""")
    sql("""create database if not exists xy""")
    sql(
      """
        | CREATE TABLE xy.fact_tablexy (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("drop datamap if exists MV_exp")
    sql("create datamap MV_exp using 'mv' as select doj,sum(salary) from xy.fact_tablexy group by doj")
    val frame = sql(
      "select doj,sum(salary) from xy.fact_tablexy group by doj")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "MV_exp"))
    sql("drop datamap if exists MV_exp")
    sql("""drop database if exists xy cascade""")
  }

  test("jira carbondata-2550") {

    sql("drop table if exists mvtable1")
    sql("drop datamap if exists map1")
    sql("create table mvtable1(name string,age int,salary int) stored by 'carbondata'")
    sql(" insert into mvtable1 select 'n1',12,12")
    sql("  insert into mvtable1 select 'n1',12,12")
    sql(" insert into mvtable1 select 'n3',12,12")
    sql(" insert into mvtable1 select 'n4',12,12")
    sql("create datamap map1 using 'mv' as select name,sum(salary) from mvtable1 group by name")
    val frame = sql("select name,sum(salary) from mvtable1 group by name limit 1")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "map1"))
    sql("drop datamap if exists map1")
    sql("drop table if exists mvtable1")
  }

  test("jira carbondata-2576") {

    sql("drop datamap if exists datamap_comp_maxsumminavg")
    sql("create datamap datamap_comp_maxsumminavg using 'mv' as select empname,max(projectenddate),sum(salary),min(projectjoindate),avg(attendance) from fact_table1 group by empname")
    val frame = sql(
      "select empname,max(projectenddate),sum(salary),min(projectjoindate),avg(attendance) from fact_table1 group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap_comp_maxsumminavg"))
    sql("drop datamap if exists datamap_comp_maxsumminavg")
  }

  test("jira carbondata-2540") {

    sql("drop datamap if exists mv_unional")
    intercept[UnsupportedOperationException] {
      sql(
        "create datamap mv_unional using 'mv' as Select Z.deptname From (Select deptname,empname From fact_table1 Union All Select deptname,empname from fact_table2) Z")
    }
    sql("drop datamap if exists mv_unional")
  }

  test("jira carbondata-2533") {

    sql("drop datamap if exists MV_exp")
    intercept[UnsupportedOperationException] {
      sql(
        "create datamap MV_exp using 'mv' as select sum(case when deptno=11 and (utilization=92) then salary else 0 end) as t from fact_table1 group by empname")

      val frame = sql(
        "select sum(case when deptno=11 and (utilization=92) then salary else 0 end) as t from fact_table1 group by empname")
      val analyzed = frame.queryExecution.analyzed
      assert(TestUtil.verifyMVDataMap(analyzed, "MV_exp"))
    }
    sql("drop datamap if exists MV_exp")
  }

  test("jira carbondata-2560") {

    sql("drop datamap if exists MV_exp1")
    sql("drop datamap if exists MV_exp2")
    sql("create datamap MV_exp1 using 'mv' as select empname, sum(utilization) from fact_table1 group by empname")
    intercept[UnsupportedOperationException] {
      sql(
        "create datamap MV_exp2 using 'mv' as select empname, sum(utilization) from fact_table1 group by empname")

    }
    sql("show datamap").show()
    val frame = sql(
      "select empname, sum(utilization) from fact_table1 group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "MV_exp1"))
    sql("drop datamap if exists MV_exp1")
    sql("drop datamap if exists MV_exp2")
  }

  test("jira carbondata-2531") {

    sql("drop datamap if exists datamap46")
    sql("create datamap datamap46 using 'mv' as select deptname, sum(salary) from fact_table1 group by deptname")
    val frame = sql(
      "select deptname as babu, sum(salary) from fact_table1 as tt group by deptname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap46"))
    sql("drop datamap if exists datamap46")
  }

  test("jira carbondata-2539") {

    sql("drop datamap if exists datamap_subqry")
    sql("create datamap datamap_subqry using 'mv' as select empname, min(salary) from fact_table1 group by empname")
    val frame = sql(
      "SELECT max(utilization) FROM fact_table1 WHERE salary IN (select min(salary) from fact_table1 group by empname ) group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap_subqry"))
    sql("drop datamap if exists datamap_subqry")
  }

  test("jira carbondata-2539-1") {
    sql("drop datamap if exists datamap_comp_maxsumminavg")
    sql("create datamap datamap_comp_maxsumminavg using 'mv' as select empname,max(projectenddate),sum(salary),min(projectjoindate),avg(attendance) from fact_table1 group by empname")
    sql("drop datamap if exists datamap_subqry")
    sql("create datamap datamap_subqry using 'mv' as select min(salary) from fact_table1")
    val frame = sql(
      "SELECT max(utilization) FROM fact_table1 WHERE salary IN (select min(salary) from fact_table1) group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap_subqry"))
    sql("drop datamap if exists datamap_subqry")
  }

  test("basic scenario") {

    sql("drop table if exists mvtable1")
    sql("drop table if exists mvtable2")
    sql("create table mvtable1(name string,age int,salary int) stored by 'carbondata'")
    sql("create table mvtable2(name string,age int,salary int) stored by 'carbondata'")
    sql("create datamap MV11 using 'mv' as select name from mvtable2")
    sql(" insert into mvtable1 select 'n1',12,12")
    sql("  insert into mvtable1 select 'n1',12,12")
    sql(" insert into mvtable1 select 'n3',12,12")
    sql(" insert into mvtable1 select 'n4',12,12")
    sql("update mvtable1 set(name) = ('updatedName')").show()
    checkAnswer(sql("select count(*) from mvtable1 where name = 'updatedName'"),Seq(Row(4)))
    sql("drop table if exists mvtable1")
    sql("drop table if exists mvtable2")
  }

  test("test create datamap with streaming table")  {
    sql("drop datamap if exists dm_stream_test1")
    sql("drop datamap if exists dm_stream_bloom")
    sql("drop datamap if exists dm_stream_PreAggMax")
    sql("drop table if exists fact_streaming_table1")
    sql(
      """
        | CREATE TABLE fact_streaming_table1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('streaming'='true')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm_stream_bloom ON TABLE fact_streaming_table1
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='empname,deptname', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    sql("create datamap dm_stream_PreAggMax on table fact_streaming_table1 using 'preaggregate' " +
        "as select empname,max(salary) as max from fact_streaming_table1 group by empname")
    
    val exception_tb_mv: Exception = intercept[Exception] {
      sql("create datamap dm_stream_test1 using 'mv' as select empname, sum(utilization) from " +
          "fact_streaming_table1 group by empname")
    }
    assert(exception_tb_mv.getMessage
      .contains("Streaming table does not support creating MV datamap"))
  }

  test("test create datamap with streaming table join carbon table and join non-carbon table ")  {
    sql("drop datamap if exists dm_stream_test2")
    sql("drop table if exists fact_streaming_table2")
    sql("drop table if exists fact_table_parquet")
    sql(
      """
        | CREATE TABLE fact_streaming_table2 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('streaming'='true')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE fact_table_parquet (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS parquet
      """.stripMargin)
    
    val exception_tb_mv2: Exception = intercept[Exception] {
      sql("create datamap dm_stream_test2 using 'mv' as select t1.empname as c1, t2.designation, " +
          "t2.empname as c2,t3.empname from (fact_table1 t1 inner join fact_streaming_table2 t2  " +
          "on (t1.empname = t2.empname)) inner join fact_table_parquet t3 " +
          "on (t1.empname = t3.empname)")
    }
    assert(exception_tb_mv2.getMessage
      .contains("Streaming table does not support creating MV datamap"))
  }

  test("test set streaming property of the table which has MV datamap")  {
    sql("drop datamap if exists dm_stream_test3")
    sql("create datamap dm_stream_test3 using 'mv' as select empname, sum(utilization) from " +
        "fact_table1 group by empname")
    val exception_tb_mv3: Exception = intercept[Exception] {
      sql("alter table fact_table1 set tblproperties('streaming'='true')")
    }
    assert(exception_tb_mv3.getMessage
      .contains("The table which has MV datamap does not support set streaming property"))
    sql("drop datamap if exists dm_stream_test3")
  }

  test("select mv stack exception") {
    val querySQL = "select sum(x12) as y1, sum(x13) as y2, sum(x14) as y3,sum(x15) " +
      "as y4,X8,x9,x1 from all_table group by X8,x9,x1"

    sql("drop datamap if exists all_table_mv")
    sql("drop table if exists all_table")

    sql("""
       | create table all_table(x1 bigint,x2 bigint,
       | x3 string,x4 bigint,x5 bigint,x6 int,x7 string,x8 int, x9 int,x10 bigint,
       | x11 bigint, x12 bigint,x13 bigint,x14 bigint,x15 bigint,x16 bigint,
       | x17 bigint,x18 bigint,x19 bigint) stored by 'carbondata'""".stripMargin)
    sql("insert into all_table select 1,1,null,1,1,1,null,1,1,1,1,1,1,1,1,1,1,1,1")

    sql("create datamap all_table_mv on table all_table using 'mv' as " + querySQL)

    val frame = sql(querySQL)
    val analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "all_table_mv"))
    assert(1 == frame.collect().size)

    sql("drop table if exists all_table")
  }

  test("test select * and distinct when MV is enabled") {
    sql("drop table if exists limit_fail")
    sql("CREATE TABLE limit_fail (empname String, designation String, doj Timestamp,workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)STORED BY 'org.apache.carbondata.format'")
    sql(s"LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE limit_fail  OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    sql("create datamap limit_fail_dm1 using 'mv' as select empname,designation from limit_fail")
    try {
      val df = sql("select distinct(empname) from limit_fail limit 10")
      sql("select * from limit_fail limit 10").show()
      val analyzed = df.queryExecution.analyzed
      assert(TestUtil.verifyMVDataMap(analyzed, "limit_fail_dm1"))
    } catch {
      case ex: Exception =>
        assert(false)
    }
  }

  test("test binary on mv") {
    val querySQL = "select x19,x20,sum(x18) from all_table group by x19, x20"
    val querySQL2 = "select x19,x20,sum(x18) from all_table where x20=cast('binary2' as binary ) group by x19, x20"

    sql("drop datamap if exists all_table_mv")
    sql("drop table if exists all_table")

    sql(
      """
        | create table all_table(x1 bigint,x2 bigint,
        | x3 string,x4 bigint,x5 bigint,x6 int,x7 string,x8 int, x9 int,x10 bigint,
        | x11 bigint, x12 bigint,x13 bigint,x14 bigint,x15 bigint,x16 bigint,
        | x17 bigint,x18 bigint,x19 bigint,x20 binary) stored by 'carbondata'""".stripMargin)
    sql("insert into all_table select 1,1,null,1,1,1,null,1,1,1,1,1,1,1,1,1,1,1,1,'binary1'")
    sql("insert into all_table select 1,1,null,1,1,1,null,1,1,1,1,1,1,1,1,1,1,12,2,'binary2'")
    sql("insert into all_table select 1,1,null,1,1,1,null,1,1,1,1,1,1,1,1,1,1,1,2,'binary2'")

    sql("create datamap all_table_mv on table all_table using 'mv' as " + querySQL)
    sql("rebuild datamap all_table_mv")

    var frame = sql(querySQL)
    var analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "all_table_mv"))
    assert(2 == frame.collect().size)
    frame.collect().foreach { each =>
      if (1 == each.get(0)) {
        assert("binary1".equals(new String(each.getAs[Array[Byte]](1))))
        assert(1 == each.get(2))
      } else if (2 == each.get(0)) {
        assert("binary2".equals(new String(each.getAs[Array[Byte]](1))))
        assert(13 == each.get(2))
      } else {
        assert(false)
      }
    }

    frame = sql(querySQL2)
    analyzed = frame.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "all_table_mv"))
    assert(1 == frame.collect().size)
    frame.collect().foreach { each =>
      if (2 == each.get(0)) {
        assert("binary2".equals(new String(each.getAs[Array[Byte]](1))))
        assert(13 == each.get(2))
      } else {
        assert(false)
      }
    }

    sql("drop table if exists all_table")
  }

  test(" test MV with like queries and filter queries") {
    sql("drop table if exists mv_like")
    sql(
      "create table mv_like(name string, age int, address string, Country string, id int) stored by 'carbondata'")
    sql(
      "create datamap mvlikedm1 using 'mv' as select name,address,sum(Country) from mv_like where Country NOT LIKE 'US' group by name,address")
    sql(
      "create datamap mvlikedm2 using 'mv' as select name,address,Country from mv_like where Country = 'US' or Country = 'China' group by name,address,Country")
    sql("insert into mv_like select 'chandler', 32, 'newYork', 'US', 5")
    val df1 = sql(
      "select name,address from mv_like where Country NOT LIKE 'US' group by name,address")
    val analyzed1 = df1.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed1, "mvlikedm1"))
    val df2 = sql(
      "select name,address,Country from mv_like where Country = 'US' or Country = 'China' group by name,address,Country")
    val analyzed2 = df2.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed2, "mvlikedm2"))
  }

  test("test distinct, count, sum on MV with single projection column") {
    sql("drop table if exists maintable")
    sql("create table maintable(name string, age int, add string) stored by 'carbondata'")
    sql("create datamap single_mv using 'mv' as select age from maintable")
    sql("insert into maintable select 'pheobe',31,'NY'")
    sql("insert into maintable select 'rachel',32,'NY'")
    val df1 = sql("select distinct(age) from maintable")
    val df2 = sql("select sum(age) from maintable")
    val df3 = sql("select count(age) from maintable")
    val analyzed1 = df1.queryExecution.analyzed
    val analyzed2 = df2.queryExecution.analyzed
    val analyzed3 = df3.queryExecution.analyzed
    checkAnswer(df1, Seq(Row(31), Row(32)))
    checkAnswer(df2, Seq(Row(63)))
    checkAnswer(df3, Seq(Row(2)))
    assert(TestUtil.verifyMVDataMap(analyzed1, "single_mv"))
    assert(TestUtil.verifyMVDataMap(analyzed2, "single_mv"))
    assert(TestUtil.verifyMVDataMap(analyzed3, "single_mv"))
  }

  test("count test case") {

    sql("drop table if exists mvtable1")
    sql("create table mvtable1(name string,age int,salary int) stored by 'carbondata'")
    sql("create datamap MV11 using 'mv' as select name from mvtable1")
    sql("insert into mvtable1 select 'n1',12,12")
    sql("rebuild datamap MV11")
    val frame = sql("select count(*) from mvtable1")
    assert(!TestUtil.verifyMVDataMap(frame.queryExecution.analyzed, "MV11"))
    checkAnswer(frame,Seq(Row(1)))
    sql("drop table if exists mvtable1")
  }

  test("test mv with duplicate columns in query and constant column") {
    sql("drop table if exists maintable")
    sql("create table maintable(name string, age int, add string) stored by 'carbondata'")
    sql("create datamap dupli_mv using 'mv' as select name, sum(age),sum(age) from maintable group by name")
    sql("create datamap dupli_projection using 'mv' as select age, age,add from maintable")
    sql("create datamap constant_mv using 'mv' as select name, sum(1) ex1 from maintable group by name")
    sql("insert into maintable select 'pheobe',31,'NY'")
    val df1 = sql("select sum(age),name from maintable group by name")
    val df2 = sql("select sum(age),sum(age),name from maintable group by name")
    val df3 = sql("select name, sum(1) ex1 from maintable group by name")
    val df4 = sql("select sum(1) ex1 from maintable group by name")
    val df5 = sql("select age,age,add from maintable")
    val df6 = sql("select age,add from maintable")
    val analyzed1 = df1.queryExecution.analyzed
    val analyzed2 = df2.queryExecution.analyzed
    val analyzed3 = df3.queryExecution.analyzed
    val analyzed4 = df4.queryExecution.analyzed
    val analyzed5 = df5.queryExecution.analyzed
    val analyzed6 = df6.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed1, "dupli_mv"))
    assert(TestUtil.verifyMVDataMap(analyzed2, "dupli_mv"))
    assert(TestUtil.verifyMVDataMap(analyzed3, "constant_mv"))
    assert(TestUtil.verifyMVDataMap(analyzed4, "constant_mv"))
    assert(TestUtil.verifyMVDataMap(analyzed5, "dupli_projection"))
    assert(TestUtil.verifyMVDataMap(analyzed6, "dupli_projection"))
  }

  test("test mv query when the column names and table name same in join scenario") {
    sql("drop table IF EXISTS price")
    sql("drop table IF EXISTS quality")
    sql("create table price(product string,price int) stored by 'carbondata'")
    sql("create table quality(product string,quality string) stored by 'carbondata'")
    sql("create datamap same_mv using 'mv' as select price.product,price.price,quality.product,quality.quality from price,quality where price.product = quality.product")
    val df1 = sql("select price.product from price,quality where price.product = quality.product")
    val analyzed1 = df1.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed1, "same_mv"))
  }

  test("test datamap column having more than 128 characters") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable (m_month smallint, c_code string, " +
        "c_country smallint, d_dollar_value double, q_quantity double, u_unit smallint, b_country smallint, i_id int, y_year smallint) stored by 'carbondata'")
    sql("insert into maintable select 10, 'xxx', 123, 456, 45, 5, 23, 1, 2000")
    sql("drop datamap if exists da_agg")
    sql("create datamap da_agg using 'mv' as select u_unit, y_year, m_month, c_country, b_country, sum(case when i_id=1 and (y_year=2000 and m_month=10)" +
        "then d_dollar_value else 0 end), sum(case when i_id=1 and (y_year=2000 and m_month=10) then q_quantity else 0 end) ex, sum(case when i_id=1 and (y_year=2011 and " +
        "(m_month>=7 and m_month <=12)) then q_quantity else 0 end) from maintable group by u_unit, y_year, m_month, c_country, b_country")
    val df = sql("select u_unit, y_year, m_month, c_country, b_country, sum(case when i_id=1 and (y_year=2000 and m_month=10) then d_dollar_value else 0 end), " +
                 "sum(case when i_id=1 and (y_year=2000 and m_month=10) then q_quantity else 0 end) ex, sum(case when i_id=1 and (y_year=2011 and (m_month>=7 and m_month " +
                 "<=12)) then q_quantity else 0 end) from maintable group by u_unit,y_year, m_month, c_country, b_country")
    val analyzed = df.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "da_agg"))
    sql("drop table IF EXISTS maintable")
  }

  test("test cast expression with mv") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable (m_month bigint, c_code string, " +
        "c_country smallint, d_dollar_value double, q_quantity double, u_unit smallint, b_country smallint, i_id int, y_year smallint) stored by 'carbondata'")
    sql("insert into maintable select 10, 'xxx', 123, 456, 45, 5, 23, 1, 2000")
    sql("drop datamap if exists da_cast")
    sql(
      "create datamap da_cast using 'mv' as select cast(floor((m_month +1000) / 900) * 900 - 2000 AS INT) as a, c_code as abc,m_month from maintable")
    val df1 = sql(
      " select cast(floor((m_month +1000) / 900) * 900 - 2000 AS INT) as a ,c_code as abc  from maintable")
    val df2 = sql(
      " select cast(floor((m_month +1000) / 900) * 900 - 2000 AS INT),c_code as abc  from maintable")
    val analyzed1 = df1.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed1, "da_cast"))
  }

  test("test cast of expression with mv") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable (m_month bigint, c_code string, " +
        "c_country smallint, d_dollar_value double, q_quantity double, u_unit smallint, b_country smallint, i_id int, y_year smallint) stored by 'carbondata'")
    sql("insert into maintable select 10, 'xxx', 123, 456, 45, 5, 23, 1, 2000")
    sql("drop datamap if exists da_cast")
    sql(
      "create datamap da_cast using 'mv' as select cast(floor((m_month +1000) / 900) * 900 - 2000 AS INT) as a, c_code as abc from maintable")
    val df1 = sql(
      " select cast(floor((m_month +1000) / 900) * 900 - 2000 AS INT) as a ,c_code as abc  from maintable")
    val df2 = sql(
      " select cast(floor((m_month +1000) / 900) * 900 - 2000 AS INT),c_code as abc  from maintable")
    val analyzed1 = df1.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed1, "da_cast"))
  }

  test("test cast with & without alias") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable (m_month bigint, c_code string, " +
        "c_country smallint, d_dollar_value double, q_quantity double, u_unit smallint, b_country smallint, i_id int, y_year smallint) stored by 'carbondata'")
    sql("insert into maintable select 10, 'xxx', 123, 456, 45, 5, 23, 1, 2000")
    sql("drop datamap if exists da_cast")
    sql(
      "create datamap da_cast using 'mv' as select cast(m_month + 1000 AS INT) as a, c_code as abc from maintable")
    checkAnswer(sql("select cast(m_month + 1000 AS INT) as a, c_code as abc from maintable"), Seq(Row(1010, "xxx")))
    var df1 = sql("select cast(m_month + 1000 AS INT) as a, c_code as abc from maintable")
    var analyzed1 = df1.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed1, "da_cast"))
    sql("drop datamap if exists da_cast")
    sql(
      "create datamap da_cast using 'mv' as select cast(m_month + 1000 AS INT), c_code from maintable")
    df1 = sql("select cast(m_month + 1000 AS INT), c_code from maintable")
    analyzed1 = df1.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed1, "da_cast"))
    checkAnswer(sql("select cast(m_month + 1000 AS INT), c_code from maintable"), Seq(Row(1010, "xxx")))
  }

  test("test mv with floor & ceil exp") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable (m_month bigint, c_code string, " +
        "c_country smallint, d_dollar_value double, q_quantity double, u_unit smallint, b_country smallint, i_id int, y_year smallint) stored by 'carbondata'")
    sql("insert into maintable select 10, 'xxx', 123, 456, 45, 5, 23, 1, 2000")
    sql("drop datamap if exists da_floor")
    sql(
      "create datamap da_floor using 'mv' as select floor(m_month) as a, c_code as abc from maintable")
    checkAnswer(sql("select floor(m_month) as a, c_code as abc from maintable"), Seq(Row(10, "xxx")))
    var df1 = sql("select floor(m_month) as a, c_code as abc from maintable")
    var analyzed1 = df1.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed1, "da_floor"))
    sql("drop datamap if exists da_ceil")
    sql(
      "create datamap da_ceil using 'mv' as select ceil(m_month) as a, c_code as abc from maintable")
    checkAnswer(sql("select ceil(m_month) as a, c_code as abc from maintable"), Seq(Row(10, "xxx")))
    var df2 = sql("select ceil(m_month) as a, c_code as abc from maintable")
    var analyzed2 = df2.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed2, "da_ceil"))
  }

  def drop(): Unit = {
    sql("drop table IF EXISTS fact_table1")
    sql("drop table IF EXISTS fact_table2")
    sql("drop table IF EXISTS fact_table3")
    sql("drop table IF EXISTS fact_table4")
    sql("drop table IF EXISTS fact_table5")
    sql("drop table IF EXISTS fact_table6")
    sql("drop table IF EXISTS fact_streaming_table1")
    sql("drop table IF EXISTS fact_streaming_table2")
    sql("drop table IF EXISTS fact_table_parquet")
    sql("drop table if exists limit_fail")
    sql("drop table IF EXISTS mv_like")
    sql("drop table IF EXISTS maintable")
  }

  test("test create datamap with add segment") {
    sql("drop table if exists fact_table_addseg")
    sql("drop table if exists fact_table_addseg1")
    sql(
      """
        | CREATE TABLE fact_table_addseg (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table_addseg OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(
      """
        | CREATE TABLE fact_table_addseg1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table_addseg1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql("drop datamap if exists datamap_addseg")
    sql("create datamap datamap_addseg using 'mv' as select empname, designation from fact_table_addseg")
    val df = sql("select empname,designation from fact_table_addseg")
    val analyzed = df.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap_addseg"))
    assert(df.collect().length == 90)
    val table = CarbonEnv.getCarbonTable(None, "fact_table_addseg1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "0")
    val newPath = storeLocation + "/" + "addsegtest"
    copy(path, newPath)

    sql(s"alter table fact_table_addseg add segment options('path'='$newPath', 'format'='carbon')").show()
    sql("select empname,designation from fact_table_addseg").show()
    val df1 = sql("select empname,designation from fact_table_addseg")
    val analyzed1 = df1.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed1, "datamap_addseg"))
    assert(df1.collect().length == 180)
    sql(s"drop datamap datamap_addseg")
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    sql("drop table if exists fact_table_addseg")
    sql("drop table if exists fact_table_addseg1")
  }

  test("test create datamap with add segment with deffered rebuild") {
    sql("drop table if exists fact_table_addseg")
    sql("drop table if exists fact_table_addseg1")
    sql(
      """
        | CREATE TABLE fact_table_addseg (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table_addseg OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(
      """
        | CREATE TABLE fact_table_addseg1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table_addseg1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql("drop datamap if exists datamap_addseg")
    sql("create datamap datamap_addseg using 'mv' WITH DEFERRED REBUILD as select empname, designation from fact_table_addseg")
    sql("rebuild datamap datamap_addseg")
    val df = sql("select empname,designation from fact_table_addseg")
    val analyzed = df.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed, "datamap_addseg"))
    assert(df.collect().length == 90)
    val table = CarbonEnv.getCarbonTable(None, "fact_table_addseg1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "0")
    val newPath = storeLocation + "/" + "addsegtest"
    copy(path, newPath)

    sql(s"alter table fact_table_addseg add segment options('path'='$newPath', 'format'='carbon')").show()
    val df1 = sql("select empname,designation from fact_table_addseg")
    val analyzed1 = df1.queryExecution.analyzed
    assert(!TestUtil.verifyMVDataMap(analyzed1, "datamap_addseg"))
    assert(df1.collect().length == 180)

    sql("rebuild datamap datamap_addseg")

    val df2 = sql("select empname,designation from fact_table_addseg")
    val analyzed2 = df2.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed2, "datamap_addseg"))
    assert(df2.collect().length == 180)

    sql(s"drop datamap datamap_addseg")
    sql("drop table if exists fact_table_addseg")
    sql("drop table if exists fact_table_addseg1")
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }



  def copy(oldLoc: String, newLoc: String): Unit = {
    val oldFolder = FileFactory.getCarbonFile(oldLoc)
    FileFactory.mkdirs(newLoc, FileFactory.getConfiguration)
    val oldFiles = oldFolder.listFiles
    for (file <- oldFiles) {
      Files.copy(Paths.get(file.getParentFile.getPath, file.getName), Paths.get(newLoc, file.getName))
    }
  }

  override def afterAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }
}

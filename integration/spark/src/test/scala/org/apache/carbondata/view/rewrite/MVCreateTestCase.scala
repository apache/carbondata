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

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedMVCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class MVCreateTestCase extends QueryTest with BeforeAndAfterAll {
  // scalastyle:off lineLength
  override def beforeAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    val projectPath = new File(this.getClass.getResource("/").getPath + "../../../../")
      .getCanonicalPath.replaceAll("\\\\", "/")
    val integrationPath = s"$projectPath/integration"
    val resourcesPath = s"$integrationPath/spark/src/test/resources"
    sql(
      """
        | CREATE TABLE fact_table1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(
      """
        | CREATE TABLE fact_table2 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(
      """
        | CREATE TABLE fact_table3 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table3 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table3 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(
      """
        | CREATE TABLE fact_table4 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table4 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table4 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(
      """
        | CREATE TABLE fact_table5 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(
      """
        | CREATE TABLE fact_table6 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table6 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table6 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
  }

  test("test if partial query with group by hits mv when all columns present in mv") {
    sql("drop table if exists sales")
    sql(" CREATE TABLE sales (id int, name string)  STORED AS carbondata")
    sql("insert into sales values(1,'ab'),(2,'bc')")
    val result1 = sql("SELECT id, name, sum(id)  FROM sales GROUP BY id, name")
    val result2 = sql("SELECT name, sum(id)  FROM sales GROUP BY id, name")
    val result3 = sql("SELECT name, sum(id)  FROM sales GROUP BY name")
    sql("drop materialized view if exists agg_sale")
    sql("CREATE MATERIALIZED VIEW agg_sale AS SELECT id, name, sum(id)  FROM sales GROUP BY id, name")
    val df1 = sql("SELECT id, name, sum(id)  FROM sales GROUP BY id, name")
    val df2 = sql("SELECT name, sum(id)  FROM sales GROUP BY id, name")
    val df3 = sql("SELECT name, sum(id)  FROM sales GROUP BY name")
    TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "agg_sale")
    TestUtil.verifyMVHit(df2.queryExecution.optimizedPlan, "agg_sale")
    TestUtil.verifyMVHit(df3.queryExecution.optimizedPlan, "agg_sale")
    checkAnswer(df1, result1)
    checkAnswer(df2, result2)
    checkAnswer(df3, result3)
    sql("drop table if exists sales")
  }

  test("test if partial query with group by hits mv when some columns present in mv") {
    sql("drop table if exists sales")
    sql(" CREATE TABLE sales (id int, name string, sal int)  STORED AS carbondata")
    sql("insert into sales values(1,'ab', 100),(2,'bc', 100)")
    val result1 = sql("SELECT id, name, sum(id)  FROM sales GROUP BY id, name")
    val result2 = sql("SELECT name, sum(id)  FROM sales GROUP BY id, name")
    sql("drop materialized view if exists agg_sale")
    sql("CREATE MATERIALIZED VIEW agg_sale AS SELECT id, name, sum(id)  FROM sales GROUP BY id, name")
    val df1 = sql("SELECT id, name, sum(id)  FROM sales GROUP BY id, name")
    val df2 = sql("SELECT name, sum(id)  FROM sales GROUP BY id, name")
    TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "agg_sale")
    TestUtil.verifyMVHit(df2.queryExecution.optimizedPlan, "agg_sale")
    checkAnswer(df1, result1)
    checkAnswer(df2, result2)
    sql("drop table if exists sales")
  }

  test("test create mv on parquet spark table") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("create table source using parquet as select * from fact_table1")
    sql("create materialized view mv1 as select empname, deptname, avg(salary) from source group by empname, deptname")
    var df = sql("select empname, avg(salary) from source group by empname")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table2 group by empname"))

    // load to parquet table and check again
    sql("insert into source select * from fact_table1")
    df = sql("select empname, avg(salary) from source group by empname")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table2 group by empname"))

    sql(s"drop materialized view mv1")
    sql("drop table source")
  }

  test("test create mv on partitioned parquet spark table") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("""
        | create table source (empname String, designation String, doj Timestamp,
        | workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, salary int)
        | using parquet partitioned by (empname)
        """.stripMargin)
    sql("insert into source select designation, doj, workgroupcategory, workgroupcategoryname, " +
        "deptno, deptname, salary, empname from fact_table1")
    sql("select * from source limit 2").collect()
    sql("create materialized view mv1 as select empname, deptname, avg(salary) from source group by empname, deptname")
    assert(sql(" select empname, deptname, avg(salary) from source group by empname, deptname limit 2").collect().length == 2)
    var df = sql("select empname, avg(salary) from source group by empname")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table2 group by empname"))

    // load to parquet table and check again
    sql("insert into source select designation, doj, workgroupcategory, workgroupcategoryname, " +
        "deptno, deptname, salary, empname from fact_table1")
    df = sql("select empname, avg(salary) from source group by empname")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table2 group by empname"))

    sql(s"drop materialized view mv1")
    sql("drop table source")
  }

  test("test create mv on orc spark table") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("create table source using orc as select * from fact_table1")
    sql("create materialized view mv1 as select empname, deptname, avg(salary) from source group by empname, deptname")
    var df = sql("select empname, avg(salary) from source group by empname")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table2 group by empname"))

    // load to orc table and check again
    sql("insert into source select * from fact_table1")
    df = sql("select empname, avg(salary) from source group by empname")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table2 group by empname"))

    sql(s"drop materialized view mv1")
    sql("drop table source")
  }

  test("test create mv on partitioned orc spark table") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("""
          | create table source (empname String, designation String, doj Timestamp,
          | workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, salary int)
          | using orc partitioned by (empname)
        """.stripMargin)
    sql("insert into source select designation, doj, workgroupcategory, workgroupcategoryname, " +
        "deptno, deptname, salary, empname from fact_table1")
    sql("select * from source limit 2").collect()
    sql("create materialized view mv1 as select empname, deptname, avg(salary) from source group by empname, deptname")
    var df = sql("select empname, avg(salary) from source group by empname")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table2 group by empname"))

    // load to parquet table and check again
    sql("insert into source select designation, doj, workgroupcategory, workgroupcategoryname, " +
        "deptno, deptname, salary, empname from fact_table1")
    df = sql("select empname, avg(salary) from source group by empname")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table2 group by empname"))

    sql(s"drop materialized view mv1")
    sql("drop table source")
  }

  test("test create mv on parquet hive table") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("create table source stored as parquet as select * from fact_table1")
    sql("create materialized view mv1 as select empname, deptname, avg(salary) from source group by empname, deptname")
    var df = sql("select empname, avg(salary) from source group by empname")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table2 group by empname"))

    // load to parquet table and check again
    sql("insert into source select * from fact_table1")
    df = sql("select empname, avg(salary) from source group by empname")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table2 group by empname"))

    sql(s"drop materialized view mv1")
    sql("drop table source")
  }

  // TODO: orc hive table is not supported since MV rewrite does not handle HiveTableRelation
  ignore("test create mv on orc hive table") {
    sql("drop materialized view if exists mv2")
    sql("drop table if exists source")
    sql("create table source stored as orc as select * from fact_table1")
    sql("explain extended select empname, avg(salary) from source group by empname").collect()
    sql("create materialized view mv2 as select empname, deptname, avg(salary) from source group by empname, deptname")
    sql("select * from mv2_table").collect()
    val df = sql("select empname, avg(salary) from source group by empname")
    sql("explain extended select empname, avg(salary) from source group by empname").collect()
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv2"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table2 group by empname"))
    sql(s"drop materialized view mv2")
    sql("drop table source")
  }

  test("test create mv with simple and same projection") {
    sql("drop materialized view if exists mv1")
    sql("create materialized view mv1 as select empname, designation from fact_table1")
    val df = sql("select empname, designation from fact_table1")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, designation from fact_table2"))
    sql(s"drop materialized view mv1")
  }

  test("test create materialized view with simple and same projection") {
    sql("drop materialized view if exists mv1")
    sql("create materialized view mv1 as select empname, designation from fact_table1")
    val df = sql("select empname,designation from fact_table1")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname,designation from fact_table2"))
    sql(s"drop materialized view mv1")
  }

  test("test create materialized view with simple and sub projection") {
    sql("drop materialized view if exists mv2")
    sql("create materialized view mv2 as select empname, designation from fact_table1")
    val df = sql("select empname from fact_table1")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv2"))
    checkAnswer(df, sql("select empname from fact_table2"))
    sql(s"drop materialized view mv2")
  }

  test("test create materialized view with simple and same projection with projection filter") {
    sql("drop materialized view if exists mv3")
    sql("create materialized view mv3 as select empname, designation from fact_table1")
    val frame = sql("select empname, designation from fact_table1 where empname='shivani'")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv3"))

    checkAnswer(frame, sql("select empname, designation from fact_table2 where empname='shivani'"))
    sql(s"drop materialized view mv3")
  }

  test("test create materialized view with simple and sub projection with non projection filter") {
    sql("create materialized view mv4 as select empname, designation from fact_table1")
    val frame = sql("select designation from fact_table1 where empname='shivani'")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv4"))
    checkAnswer(frame, sql("select designation from fact_table2 where empname='shivani'"))
    sql(s"drop materialized view mv4")
  }

  test("test create materialized view with simple and sub projection with filter") {
    sql("create materialized view mv5 as select empname, designation from fact_table1 where empname='shivani'")
    val frame = sql("select designation from fact_table1 where empname='shivani'")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv5"))
    assert(sql("select designation from fact_table1 where empname='shivani' limit 5").collect().length == 5)
    checkAnswer(frame, sql("select designation from fact_table2 where empname='shivani'"))
    sql(s"drop materialized view mv5")
  }

  test("test create materialized view with simple and same projection with filter ") {
    sql("create materialized view mv6 as select empname, designation from fact_table1 where empname='shivani'")
    val frame = sql("select empname,designation from fact_table1 where empname='shivani'")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv6"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where empname='shivani'"))
    sql(s"drop materialized view mv6")
  }

  test("test create materialized view with simple and same projection with filter and extra query column filter") {
    sql("create materialized view mv7 as select empname, designation from fact_table1 where empname='shivani'")
    val frame = sql(
      "select empname,designation from fact_table1 where empname='shivani' and designation='SA'")
    assert( sql("select empname,designation from fact_table1 where empname='shivani' and designation='SA' limit 1").collect().length == 0)
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv7"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where empname='shivani' and designation='SA'"))
    sql(s"drop materialized view mv7")
  }

  test("test create materialized view with simple and same projection with filter and different column filter") {
    sql("create materialized view mv8 as select empname, designation from fact_table1 where empname='shivani'")
    val frame = sql("select empname,designation from fact_table1 where designation='SA'")
    assert(!TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv8"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where designation='SA'"))
    sql(s"drop materialized view mv8")
  }

  test("test create materialized view with simple and same projection with filter on non projection column and extra column filter") {
    sql("create materialized view mv9 as select empname, designation,deptname  from fact_table1 where deptname='cloud'")
    val frame = sql("select empname,designation from fact_table1 where deptname='cloud'")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv9"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where deptname='cloud'"))
    sql(s"drop materialized view mv9")
  }

  test("test create materialized view with simple and same projection with filter on non projection column and no column filter") {
    sql("create materialized view mv10 as select empname, designation,deptname from fact_table1 where deptname='cloud'")
    val frame = sql("select empname,designation from fact_table1")
    assert(!TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv10"))
    checkAnswer(frame, sql("select empname,designation from fact_table2"))
    sql(s"drop materialized view mv10")
  }

  test("test create materialized view with simple and same projection with filter on non projection column and different column filter") {
    sql("create materialized view mv11 as select empname, designation,deptname from fact_table1 where deptname='cloud'")
    val frame = sql("select empname,designation from fact_table1 where designation='SA'")
    assert(!TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv11"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where designation='SA'"))
    sql(s"drop materialized view mv11")
  }

  test("test create materialized view with simple and same group by query") {
    sql("drop materialized view if exists mv12")
    sql("create materialized view mv12 as select empname, sum(utilization) from fact_table1 group by empname")
    val frame = sql("select empname, sum(utilization) from fact_table1 group by empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv12"))
    checkAnswer(frame, sql("select empname, sum(utilization) from fact_table2 group by empname"))
    sql(s"drop materialized view mv12")
  }

  test("test create materialized view with simple and sub group by query") {
    sql("drop materialized view if exists mv13")
    sql("create materialized view mv13 as select empname, sum(utilization) from fact_table1 group by empname")
    val frame = sql("select sum(utilization) from fact_table1 group by empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv13"))
    checkAnswer(frame, sql("select sum(utilization) from fact_table2 group by empname"))
    sql(s"drop materialized view mv13")
  }

  test("test create materialized view with simple and sub group by query with filter on query") {
    sql("drop materialized view if exists mv14")
    sql("create materialized view mv14 as select empname, sum(utilization) from fact_table1 group by empname")
    val frame = sql(
      "select empname,sum(utilization) from fact_table1 group by empname having empname='shivani'")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv14"))
    checkAnswer(frame, sql("select empname,sum(utilization) from fact_table2 where empname='shivani' group by empname"))
    sql(s"drop materialized view mv14")
  }

  test("test create materialized view with simple and sub group and sub projection by query with filter on query") {
    sql("drop materialized view if exists mv32")
    sql("create materialized view mv32 as select empname, sum(utilization) from fact_table1 group by empname")
    val frame = sql(
      "select empname, sum(utilization) from fact_table1 group by empname having empname='shivani'")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv32"))
    checkAnswer(frame, sql( "select empname, sum(utilization) from fact_table2 group by empname having empname='shivani'"))
    sql(s"drop materialized view mv32")
  }

  test("test create materialized view having sub-query alias used in projection") {
    sql("drop materialized view if exists mv_sub")
    val subQuery = "select empname, sum(result) sum_ut from " +
                   "(select empname, utilization result from fact_table1) fact_table1 " +
                   "group by empname"
    sql("create materialized view mv_sub as " + subQuery)
    val frame = sql(subQuery)
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv_sub"))
    checkAnswer(frame,
      sql("select empname, sum(result) ut from (select empname, utilization result from " +
          "fact_table2) fact_table2 group by empname"))
    sql(s"drop materialized view mv_sub")
  }

  test("test create materialized view used as sub-query in actual query") {
    sql("drop materialized view if exists mv_sub")
    sql("create materialized view mv_sub as select empname, utilization result from fact_table1")
    val df1 = sql("select empname, sum(result) sum_ut from " +
                  "(select empname, utilization result from fact_table1) fact_table1 " +
                  "group by empname")
    val df2 = sql("select emp, sum(result) sum_ut from " +
                  "(select empname emp, utilization result from fact_table1) fact_table1 " +
                  "group by emp")
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "mv_sub"))
    assert(TestUtil.verifyMVHit(df2.queryExecution.optimizedPlan, "mv_sub"))
    checkAnswer(df1,
      sql("select empname, sum(result) ut from (select empname, utilization result from " +
          "fact_table2) fact_table2 group by empname"))
    checkAnswer(df2,
      sql("select emp, sum(result) ut from (select empname emp, utilization result from " +
          "fact_table2) fact_table2 group by emp"))
    sql(s"drop materialized view mv_sub")
  }

  test("test create materialized view with simple and sub group by query with filter on materialized view") {
    sql("create materialized view mv15 as select empname, sum(utilization) from fact_table1 where empname='shivani' group by empname")
    val frame = sql(
      "select empname,sum(utilization) from fact_table1 where empname='shivani' group by empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv15"))
    checkAnswer(frame, sql("select empname,sum(utilization) from fact_table2 where empname='shivani' group by empname"))
    sql(s"drop materialized view mv15")
  }

  test("test create materialized view with simple and sub group by query with filter on materialized view and no filter on query") {
    sql("create materialized view mv16 as select empname, sum(utilization) from fact_table1 where empname='shivani' group by empname")
    val frame = sql("select empname,sum(utilization) from fact_table1 group by empname")
    assert(!TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv16"))
    checkAnswer(frame, sql("select empname,sum(utilization) from fact_table2 group by empname"))
    sql(s"drop materialized view mv16")
  }

  test("test create materialized view with simple and same group by with expression") {
    sql("create materialized view mv17 as select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    val frame = sql(
      "select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group" +
      " by empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv17"))
    assert(sql("select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname limit 2").collect().length == 2)
    checkAnswer(frame, sql("select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table2 group" +
                           " by empname"))
    sql(s"drop materialized view mv17")
  }

  test("test create materialized view with simple and sub group by with expression") {
    sql("drop materialized view if exists mv18")
    sql("create materialized view mv18 as select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    val frame = sql(
      "select sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv18"))
    checkAnswer(frame, sql("select sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table2 group by empname"))
    sql(s"drop materialized view mv18")
  }

  test("test create materialized view with simple and sub count group by with expression") {
    sql("drop materialized view if exists mv19")
    sql("create materialized view mv19 as select empname, count(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    val frame = sql(
      "select count(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv19"))
    checkAnswer(frame, sql("select count(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table2 group by empname"))
    sql(s"drop materialized view mv19")
  }

  test("test create materialized view with simple and sub group by with expression and filter on query") {
    sql("drop materialized view if exists mv20")
    sql("create materialized view mv20 as select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    val frame = sql(
      "select sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 where " +
      "empname='shivani' group by empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv20"))
    checkAnswer(frame, sql("select sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table2 where " +
                           "empname='shivani' group by empname"))
    sql(s"drop materialized view mv20")
  }

  test("test create materialized view with simple join") {
    sql("drop materialized view if exists mv21")
    sql("create materialized view mv21 as select t1.empname as c1, t2.designation, t2.empname as c2 from fact_table1 t1 inner join fact_table2 t2  on (t1.empname = t2.empname)")
    val frame = sql(
      "select t1.empname as c1, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = t2.empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv21"))
    assert(sql("select sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname limit 2").collect().length == 2)
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = t2.empname"))
    sql(s"drop materialized view mv21")
  }

  test("test create materialized view with simple join and filter on query") {
    sql("drop materialized view if exists mv22")
    sql("create materialized view mv22 as select t1.empname, t2.designation,t2.empname from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname)")
    val frame = sql(
      "select t1.empname, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = " +
      "t2.empname and t1.empname='shivani'")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv22"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = " +
                           "t2.empname and t1.empname='shivani'"))
    sql(s"drop materialized view mv22")
  }


  test("test create materialized view with simple join and filter on query and materialized view") {
    sql("drop materialized view if exists mv23")
    sql("create materialized view mv23 as select t1.empname, t2.designation, t2.empname from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname) where t1.empname='shivani'")
    val frame = sql(
      "select t1.empname, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = " +
      "t2.empname and t1.empname='shivani'")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv23"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = " +
                           "t2.empname and t1.empname='shivani'"))
    sql(s"drop materialized view mv23")
  }

  test("test create materialized view with simple join and filter on materialized view and no filter on query") {
    sql("drop materialized view if exists mv24")
    sql("create materialized view mv24 as select t1.empname, t2.designation, t2.empname from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname) where t1.empname='shivani'")
    val frame = sql(
      "select t1.empname, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = t2.empname")
    assert(!TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv24"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = t2.empname"))
    sql(s"drop materialized view mv24")
  }

  test("test create materialized view with multiple join") {
    sql("drop materialized view if exists mv25")
    sql("create materialized view mv25 as select t1.empname as c1, t2.designation, t2.empname, t3.empname from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname) inner join fact_table3 t3  on (t1.empname=t3.empname)")
    val frame = sql(
      "select t1.empname as c1, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = t2.empname")
    assert(!TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv25"))
    val frame1 = sql(
      "select t1.empname as c1, t2.designation from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname) inner join fact_table3 t3  on (t1.empname=t3.empname)")
    assert(TestUtil.verifyMVHit(frame1.queryExecution.optimizedPlan, "mv25"))
    assert(sql("select t1.empname as c1, t2.designation from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname) inner join fact_table3 t3  on (t1.empname=t3.empname) limit 2").collect().length == 2)
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = t2.empname"))
    sql(s"drop materialized view mv25")
  }

  ignore("test create materialized view with simple join on materialized view and multi join on query") {
    sql("create materialized view mv26 as select t1.empname, t2.designation, t2.empname from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname)")
    val frame = sql(
      "select t1.empname, t2.designation from fact_table1 t1,fact_table2 t2,fact_table3 " +
      "t3  where t1.empname = t2.empname and t1.empname=t3.empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv26"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2,fact_table6 " +
                           "t3  where t1.empname = t2.empname and t1.empname=t3.empname"))
    sql(s"drop materialized view mv26")
  }

  test("test create materialized view with join with group by") {
    sql("create materialized view mv27 as select  t1.empname , t2.designation, sum(t1.utilization), sum(t2.empname) from fact_table1 t1 inner join fact_table2 t2  on (t1.empname = t2.empname) group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname group by t1.empname, t2.designation")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv27"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  " +
                           "where t1.empname = t2.empname group by t1.empname, t2.designation"))
    assert(sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where t1.empname = t2.empname group by t1.empname, t2.designation limit 2").collect().length == 2)
    sql(s"drop materialized view mv27")
  }

  test("test create materialized view with join with group by and sub projection") {
    sql("drop materialized view if exists mv28")
    sql("create materialized view mv28 as select t1.empname, t2.designation, sum(t1.utilization),sum(t2.empname) from fact_table1 t1 inner join fact_table2 t2  on (t1.empname = t2.empname) group by t1.empname, t2.designation")
    val frame = sql(
      "select t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where " +
      "t1.empname = t2.empname group by t2.designation")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv28"))
    checkAnswer(frame, sql("select t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  where " +
                           "t1.empname = t2.empname group by t2.designation"))
    sql(s"drop materialized view mv28")
  }

  test("test create materialized view with join with group by and sub projection with filter") {
    sql("drop materialized view if exists mv29")
    sql("create materialized view mv29 as select t1.empname, t2.designation, sum(t1.utilization),sum(t2.empname) from fact_table1 t1 inner join fact_table2 t2  on (t1.empname = t2.empname) group by t1.empname, t2.designation")
    val frame = sql(
      "select t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where " +
      "t1.empname = t2.empname and t1.empname='shivani' group by t2.designation")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv29"))
    checkAnswer(frame, sql("select t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  where " +
                           "t1.empname = t2.empname and t1.empname='shivani' group by t2.designation"))
    sql(s"drop materialized view mv29")
  }

  test("test create materialized view with join with group by and projection with filter") {
    sql("drop materialized view if exists mv29")
    sql("create materialized view mv29 as select t1.empname, t2.designation, sum(t1.utilization),sum(t2.empname) from fact_table1 t1 inner join fact_table2 t2  on (t1.empname = t2.empname) group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname ,t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where " +
      "t1.empname = t2.empname and t1.empname='shivani' group by t2.designation,t1.empname ")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv29"))
    checkAnswer(frame, sql("select t1.empname ,t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  where " +
                           "t1.empname = t2.empname and t1.empname='shivani' group by t2.designation,t1.empname "))
    sql(s"drop materialized view mv29")
  }

  test("test create materialized view with join with group by and sub projection with filter with alias") {
    sql("drop materialized view if exists mv29")
    sql("create materialized view mv29 as select t1.empname as a, t2.designation as b, sum(t1.utilization),sum(t2.empname) from fact_table1 t1 inner join fact_table2 t2  on (t1.empname = t2.empname) group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname ,t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where " +
      "t1.empname = t2.empname and t1.empname='shivani' group by t2.designation,t1.empname ")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv29"))
    checkAnswer(frame, sql("select t1.empname ,t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  where " +
                           "t1.empname = t2.empname and t1.empname='shivani' group by t2.designation,t1.empname "))
    sql(s"drop materialized view mv29")
  }

  test("test create materialized view with join with group by with filter") {
    sql("drop materialized view if exists mv30")
    sql("create materialized view mv30 as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 inner join fact_table2 t2 on (t1.empname = t2.empname) group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname and t2.designation='SA' group by t1.empname, t2.designation")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv30"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  " +
                           "where t1.empname = t2.empname and t2.designation='SA' group by t1.empname, t2.designation"))
    sql(s"drop materialized view mv30")
  }

  ignore("test create materialized view with expression on projection") {
    sql(s"drop materialized view if exists mv31")
    sql("create materialized view mv31 as select empname, designation, utilization, projectcode from fact_table1 ")
    val frame = sql(
      "select empname, designation, utilization+projectcode from fact_table1")
    assert(!TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv31"))
    checkAnswer(frame, sql("select empname, designation, utilization+projectcode from fact_table2"))
    sql(s"drop materialized view mv31")
  }

  test("test create materialized view with simple and sub group by query and count agg") {
    sql(s"drop materialized view if exists mv32")
    sql("create materialized view mv32 as select empname, count(utilization) from fact_table1 group by empname")
    val frame = sql("select empname,count(utilization) from fact_table1 where empname='shivani' group by empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv32"))
    checkAnswer(frame, sql("select empname,count(utilization) from fact_table2 where empname='shivani' group by empname"))
    sql(s"drop materialized view mv32")
  }

  test("test create materialized view with simple and sub group by query and avg agg") {
    sql(s"drop materialized view if exists mv33")
    sql("create materialized view mv33 as select empname, avg(utilization) from fact_table1 group by empname")
    val frame = sql("select empname,avg(utilization) from fact_table1 where empname='shivani' group by empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv33"))
    checkAnswer(frame, sql("select empname,avg(utilization) from fact_table2 where empname='shivani' group by empname"))
    sql(s"drop materialized view mv33")
  }

  ignore("test create materialized view with left join with group by") {
    sql("drop materialized view if exists mv34")
    sql("create materialized view mv34 as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname group by t1.empname, t2.designation")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv34"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname group by t1.empname, t2.designation"))
    sql(s"drop materialized view mv34")
  }

  test("test create materialized view with simple and group by query with filter on materialized view but not on projection") {
    sql("create materialized view mv35 as select designation, sum(utilization) from fact_table1 where empname='shivani' group by designation")
    val frame = sql(
      "select designation, sum(utilization) from fact_table1 where empname='shivani' group by designation")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv35"))
    checkAnswer(frame, sql("select designation, sum(utilization) from fact_table2 where empname='shivani' group by designation"))
    sql(s"drop materialized view mv35")
  }

  test("test create materialized view with simple and sub group by query with filter on materialized view but not on projection") {
    sql("create materialized view mv36 as select designation, sum(utilization) from fact_table1 where empname='shivani' group by designation")
    val frame = sql(
      "select sum(utilization) from fact_table1 where empname='shivani' group by designation")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv36"))
    checkAnswer(frame, sql("select sum(utilization) from fact_table2 where empname='shivani' group by designation"))
    sql(s"drop materialized view mv36")
  }

  test("test create materialized view with agg push join with sub group by ") {
    sql("drop materialized view if exists mv37")
    sql("create materialized view mv37 as select empname, designation, sum(utilization) from fact_table1 group by empname, designation")
    val frame = sql(
      "select t1.empname, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname group by t1.empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv37"))
    checkAnswer(frame, sql("select t1.empname, sum(t1.utilization) from fact_table3 t1,fact_table4 t2  " +
                           "where t1.empname = t2.empname group by t1.empname, t1.designation"))
    sql(s"drop materialized view mv37")
  }

  test("test create materialized view with agg push join with group by ") {
    sql("drop materialized view if exists mv38")
    sql("create materialized view mv38 as select empname, designation, sum(utilization) from fact_table1 group by empname, designation")
    val frame = sql(
      "select t1.empname, t1.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname group by t1.empname,t1.designation")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv38"))
    checkAnswer(frame, sql("select t1.empname,t1.designation, sum(t1.utilization) from fact_table3 t1,fact_table4 t2  " +
                           "where t1.empname = t2.empname group by t1.empname, t1.designation"))
    sql(s"drop materialized view mv38")
  }

  test("test create materialized view with agg push join with group by with filter") {
    sql("drop materialized view if exists mv39")
    sql("create materialized view mv39 as select empname, designation, sum(utilization) from fact_table1 group by empname, designation ")
    val frame = sql(
      "select t1.empname, t1.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname and t1.empname='shivani' group by t1.empname,t1.designation")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv39"))
    checkAnswer(frame, sql("select t1.empname,t1.designation, sum(t1.utilization) from fact_table3 t1,fact_table4 t2  " +
                           "where t1.empname = t2.empname and t1.empname='shivani' group by t1.empname, t1.designation"))
    sql(s"drop materialized view mv39")
  }

  test("test create materialized view with more agg push join with group by with filter") {
    sql("drop materialized view if exists mv40")
    sql("create materialized view mv40 as select empname, designation, sum(utilization), count(utilization) from fact_table1 group by empname, designation ")
    val frame = sql(
      "select t1.empname, t1.designation, sum(t1.utilization),count(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname and t1.empname='shivani' group by t1.empname,t1.designation")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv40"))
    checkAnswer(frame, sql("select t1.empname, t1.designation, sum(t1.utilization),count(t1.utilization) from fact_table3 t1,fact_table4 t2  " +
                           "where t1.empname = t2.empname and t1.empname='shivani' group by t1.empname,t1.designation"))
    sql(s"drop materialized view mv40")
  }

  test("test create materialized view with left join with group by with filter") {
    sql("drop materialized view if exists mv41")
    sql("create materialized view mv41 as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname, t2.designation")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv41"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname, t2.designation"))
    sql(s"drop materialized view mv41")
  }

  test("test create materialized view with left join with sub group by") {
    sql("drop materialized view if exists mv42")
    sql("create materialized view mv42 as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname group by t1.empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv42"))
    checkAnswer(frame, sql("select t1.empname, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname group by t1.empname"))
    sql(s"drop materialized view mv42")
  }

  test("test create materialized view with left join with sub group by with filter") {
    sql("drop materialized view if exists mv43")
    sql("create materialized view mv43 as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv43"))
    checkAnswer(frame, sql("select t1.empname, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname"))
    sql(s"drop materialized view mv43")
  }

  test("test create materialized view with left join with sub group by with filter on mv") {
    sql("drop materialized view if exists mv44")
    sql("create materialized view mv44 as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname, t2.designation")
    val frame = sql(
      "select t1.empname, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv44"))
    checkAnswer(frame, sql("select t1.empname, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname"))
    sql(s"drop materialized view mv44")
  }

  test("test create materialized view with left join on query and equi join on mv with group by with filter") {
    sql("drop materialized view if exists mv45")

    sql("create materialized view mv45 as select t1.empname, t2.designation, sum(t1.utilization),sum(t2.empname) from fact_table1 t1 join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    // During spark optimizer it converts the left outer join queries with equi join if any filter present on right side table
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname where t2.designation='SA' group by t1.empname, t2.designation")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv45"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname where t2.designation='SA' group by t1.empname, t2.designation"))
    sql(s"drop materialized view mv45")
  }

  test("jira carbondata-2523") {

    sql("drop materialized view if exists mv13")
    sql("drop table if exists test4")
    sql("create table test4 ( name string,age int,salary int) STORED AS carbondata")

    sql(" insert into test4 select 'babu',12,12").collect()
    sql("create materialized view mv13 as select name,sum(salary) from test4 group by name")
    val frame = sql(
      "select name,sum(salary) from test4 group by name")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv13"))
  }

  test("jira carbondata-2528-1") {

    sql("drop materialized view if exists MV_order")
    sql("create materialized view MV_order as select empname,sum(salary) as total from fact_table1 group by empname")
    val frame = sql(
      "select empname,sum(salary) as total from fact_table1 group by empname order by empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "MV_order"))
    assert(sql("select empname,sum(salary) as total from fact_table1 group by empname order by empname limit 2").collect().length == 2)
  }

  test("jira carbondata-2528-2") {
    sql("drop materialized view if exists MV_order")
    sql("drop materialized view if exists MV_desc_order")
    sql("create materialized view MV_order as select empname,sum(salary)+sum(utilization) as total from fact_table1 group by empname")
    val frame = sql(
      "select empname,sum(salary)+sum(utilization) as total from fact_table1 group by empname order by empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "MV_order"))
  }

  test("jira carbondata-2528-3") {

    sql("drop materialized view if exists MV_order")
    sql("create materialized view MV_order as select empname,sum(salary)+sum(utilization) as total from fact_table1 group by empname order by empname DESC")
    val frame = sql(
      "select empname,sum(salary)+sum(utilization) as total from fact_table1 group by empname order by empname DESC")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "MV_order"))
    sql("drop materialized view if exists MV_order")
  }

  test("jira carbondata-2528-4") {

    sql("drop materialized view if exists MV_order")
    sql("create materialized view MV_order as select empname,sum(salary)+sum(utilization) as total from fact_table1 group by empname order by empname DESC")
    val frame = sql(
      "select empname,sum(salary)+sum(utilization) as total from fact_table1 where empname = 'ravi' group by empname order by empname DESC")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "MV_order"))
    sql("drop materialized view if exists MV_order")
  }

  test("jira carbondata-2530") {

    sql("drop table if exists test1")
    sql("drop materialized view if exists datamv2")
    sql("create table test1( name string,country string,age int,salary int) STORED AS carbondata")
    sql("insert into test1 select 'name1','USA',12,23")
    sql("create materialized view datamv2 as select country,sum(salary) from test1 group by country")
    val frame = sql("select country,sum(salary) from test1 where country='USA' group by country")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "datamv2"))
    sql("insert into test1 select 'name1','USA',12,23")
    val frame1 = sql("select country,sum(salary) from test1 where country='USA' group by country")
    assert(TestUtil.verifyMVHit(frame1.queryExecution.optimizedPlan, "datamv2"))
    sql("drop materialized view if exists datamv2")
    sql("drop table if exists test1")
  }

  test("jira carbondata-2534") {

    sql("drop materialized view if exists MV_exp")
    sql("create materialized view MV_exp as select sum(salary),substring(empname,2,5),designation from fact_table1 group by substring(empname,2,5),designation")
    val frame = sql(
      "select sum(salary),substring(empname,2,5),designation from fact_table1 group by substring(empname,2,5),designation")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "MV_exp"))
    sql("drop materialized view if exists MV_exp")
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
        | STORED AS carbondata
      """.stripMargin)
    sql("drop materialized view if exists MV_exp")
    sql("create materialized view MV_exp as select doj,sum(salary) from xy.fact_tablexy group by doj")
    val frame = sql(
      "select doj,sum(salary) from xy.fact_tablexy group by doj")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "MV_exp"))
    sql("drop materialized view if exists MV_exp")
    sql("""drop database if exists xy cascade""")
  }

  test("jira carbondata-2550") {

    sql("drop table if exists mvtable1")
    sql("drop materialized view if exists map1")
    sql("create table mvtable1(name string,age int,salary int) STORED AS carbondata")
    sql(" insert into mvtable1 select 'n1',12,12")
    sql("  insert into mvtable1 select 'n1',12,12")
    sql(" insert into mvtable1 select 'n3',12,12")
    sql(" insert into mvtable1 select 'n4',12,12")
    sql("create materialized view map1 as select name,sum(salary) from mvtable1 group by name")
    val frame = sql("select name,sum(salary) from mvtable1 group by name limit 1")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "map1"))
    sql("drop materialized view if exists map1")
    sql("drop table if exists mvtable1")
  }

  test("jira carbondata-2576") {

    sql("drop materialized view if exists  comp_maxsumminavg")
    sql("create materialized view comp_maxsumminavg as select empname,max(projectenddate),sum(salary),min(projectjoindate),avg(attendance) from fact_table1 group by empname")
    val frame = sql(
      "select empname,max(projectenddate),sum(salary),min(projectjoindate),avg(attendance) from fact_table1 group by empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "comp_maxsumminavg"))
    sql("drop materialized view if exists comp_maxsumminavg")
  }

  test("jira carbondata-2540") {

    sql("drop materialized view if exists mv_unional")
    intercept[UnsupportedOperationException] {
      sql(
        "create materialized view mv_unional as Select Z.deptname From (Select deptname,empname From fact_table1 Union All Select deptname,empname from fact_table2) Z")
    }
    sql("drop materialized view if exists mv_unional")
  }

  test("jira carbondata-2533") {

    sql("drop materialized view if exists MV_exp")
    intercept[UnsupportedOperationException] {
      sql(
        "create materialized view MV_exp as select sum(case when deptno=11 and (utilization=92) then salary else 0 end) as t from fact_table1 group by empname")

      val frame = sql(
        "select sum(case when deptno=11 and (utilization=92) then salary else 0 end) as t from fact_table1 group by empname")
      assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "MV_exp"))
    }
    sql("drop materialized view if exists MV_exp")
  }

//  test("jira carbondata-2560") {
//
//    sql("drop materialized view if exists MV_exp2")
//    sql("create materialized view MV_exp1 as select empname, sum(utilization) from fact_table1 group by empname")
//    intercept[UnsupportedOperationException] {
//      sql(
//        "create materialized view MV_exp2 as select empname, sum(utilization) from fact_table1 group by empname")
//
//    }
//    sql("show materialized views").collect()
//    val frame = sql(
//      "select empname, sum(utilization) from fact_table1 group by empname")
//    assert(TestUtil.verifyMV(frame.queryExecution.optimizedPlan, "MV_exp1"))
//    sql("drop materialized view if exists MV_exp1")
//    sql("drop materialized view if exists MV_exp2")
//  }

  test("jira carbondata-2531") {

    sql("drop materialized view if exists mv46")
    sql("create materialized view mv46 as select deptname, sum(salary) from fact_table1 group by deptname")
    val frame = sql(
      "select deptname as babu, sum(salary) from fact_table1 as tt group by deptname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "mv46"))
    sql("drop materialized view if exists mv46")
  }

  test("jira carbondata-2539") {

    sql("drop materialized view if exists subqry")
    sql("create materialized view subqry as select empname, min(salary) from fact_table1 group by empname")
    val frame = sql(
      "SELECT max(utilization) FROM fact_table1 WHERE salary IN (select min(salary) from fact_table1 group by empname ) group by empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "subqry"))
    sql("drop materialized view if exists subqry")
  }

  test("jira carbondata-2539-1") {
    sql("drop materialized view if exists subqry")
    sql("create materialized view subqry as select empname,max(projectenddate),sum(salary),min(projectjoindate),avg(attendance) from fact_table1 group by empname")
    sql("drop materialized view if exists subqry")
    sql("create materialized view subqry as select min(salary) from fact_table1")
    val frame = sql(
      "SELECT max(utilization) FROM fact_table1 WHERE salary IN (select min(salary) from fact_table1) group by empname")
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "subqry"))
    sql("drop materialized view if exists subqry")
  }

  test("basic scenario") {

    sql("drop table if exists mvtable1")
    sql("drop table if exists mvtable2")
    sql("create table mvtable1(name string,age int,salary int) STORED AS carbondata")
    sql("create table mvtable2(name string,age int,salary int) STORED AS carbondata")
    sql("create materialized view MV11 as select name from mvtable2")
    sql(" insert into mvtable1 select 'n1',12,12")
    sql("  insert into mvtable1 select 'n1',12,12")
    sql(" insert into mvtable1 select 'n3',12,12")
    sql(" insert into mvtable1 select 'n4',12,12")
    sql("update mvtable1 set(name) = ('updatedName')").collect()
    checkAnswer(sql("select count(*) from mvtable1 where name = 'updatedName'"), Seq(Row(4)))
    sql(s"drop materialized view MV11")
    sql("drop table if exists mvtable1")
    sql("drop table if exists mvtable2")
  }

  test("test create materialized view with streaming table")  {
    sql("drop materialized view if exists dm_stream_test1")
    sql("drop materialized view if exists dm_stream_bloom")
    sql("drop table if exists fact_streaming_table1")
    sql(
      """
        | CREATE TABLE fact_streaming_table1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
        | tblproperties('streaming'='true')
      """.stripMargin)
    sql(
      s"""
         | CREATE INDEX dm_stream_bloom
         | ON TABLE fact_streaming_table1 (EMPNAME, DEPTNAME)
         | AS 'bloomfilter'
         | Properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    val exception_tb_mv: Exception = intercept[Exception] {
      sql("create materialized view dm_stream_test1 as select empname, sum(utilization) from " +
          "fact_streaming_table1 group by empname")
    }
    assert(exception_tb_mv.getMessage
      .contains("Cannot create mv on stream table default_fact_streaming_table1"))
  }

  test("test create materialized view with streaming table join carbon table and join non-carbon table ")  {
    sql("drop materialized view if exists dm_stream_test2")
    sql("drop table if exists fact_streaming_table2")
    sql("drop table if exists fact_table_parquet")
    sql(
      """
        | CREATE TABLE fact_streaming_table2 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
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
      sql("create materialized view dm_stream_test2 as select t1.empname as c1, t2.designation, " +
          "t2.empname as c2,t3.empname from (fact_table1 t1 inner join fact_streaming_table2 t2  " +
          "on (t1.empname = t2.empname)) inner join fact_table_parquet t3 " +
          "on (t1.empname = t3.empname)")
    }
    assert(exception_tb_mv2.getMessage
      .contains("Cannot create mv on stream table default_fact_streaming_table2"))
  }

  test("test set streaming property of the table which has MV materialized view")  {
    sql("drop materialized view if exists dm_stream_test3")
    sql("create materialized view dm_stream_test3 as select empname, sum(utilization) from " +
        "fact_table1 group by empname")
    val exception_tb_mv3: Exception = intercept[Exception] {
      sql("alter table fact_table1 set tblproperties('streaming'='true')")
    }
    assert(exception_tb_mv3.getMessage
      .contains("The table which has materialized view does not support set streaming property"))
    sql("drop materialized view if exists dm_stream_test3")
  }

  test("select mv stack exception") {
    val querySQL = "select sum(x12) as y1, sum(x13) as y2, sum(x14) as y3,sum(x15) " +
      "as y4,X8,x9,x1 from all_table group by X8,x9,x1"

    sql("drop materialized view if exists all_table_mv")
    sql("drop table if exists all_table")

    sql("""
       | create table all_table(x1 bigint,x2 bigint,
       | x3 string,x4 bigint,x5 bigint,x6 int,x7 string,x8 int, x9 int,x10 bigint,
       | x11 bigint, x12 bigint,x13 bigint,x14 bigint,x15 bigint,x16 bigint,
       | x17 bigint,x18 bigint,x19 bigint) STORED AS carbondata""".stripMargin)
    sql("insert into all_table select 1,1,null,1,1,1,null,1,1,1,1,1,1,1,1,1,1,1,1")

    sql("create materialized view all_table_mv as " + querySQL)

    val frame = sql(querySQL)
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "all_table_mv"))
    assert(1 == frame.collect().size)

    sql("drop table if exists all_table")
  }

  test("test select * and distinct when MV is enabled") {
    sql("drop table if exists limit_fail")
    sql("CREATE TABLE limit_fail (empname String, designation String, doj Timestamp,workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)STORED AS carbondata")
    sql(s"LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE limit_fail  OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    sql("create materialized view limit_fail_dm1 as select empname,designation from limit_fail")
    try {
      val df = sql("select distinct(empname) from limit_fail limit 10")
      sql("select * from limit_fail limit 10").collect()
      assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "limit_fail_dm1"))
    } catch {
      case ex: Exception =>
        assert(false)
    }
  }

  test("test binary on mv") {
    val querySQL = "select x19,x20,sum(x18) from all_table group by x19, x20"
    val querySQL2 = "select x19,x20,sum(x18) from all_table where x20=cast('binary2' as binary ) group by x19, x20"

    sql("drop materialized view if exists all_table_mv")
    sql("drop table if exists all_table")

    sql(
      """
        | create table all_table(x1 bigint,x2 bigint,
        | x3 string,x4 bigint,x5 bigint,x6 int,x7 string,x8 int, x9 int,x10 bigint,
        | x11 bigint, x12 bigint,x13 bigint,x14 bigint,x15 bigint,x16 bigint,
        | x17 bigint,x18 bigint,x19 bigint,x20 binary) STORED AS carbondata""".stripMargin)
    sql("insert into all_table select 1,1,null,1,1,1,null,1,1,1,1,1,1,1,1,1,1,1,1,'binary1'")
    sql("insert into all_table select 1,1,null,1,1,1,null,1,1,1,1,1,1,1,1,1,1,12,2,'binary2'")
    sql("insert into all_table select 1,1,null,1,1,1,null,1,1,1,1,1,1,1,1,1,1,1,2,'binary2'")

    sql("create materialized view all_table_mv as " + querySQL)
    sql("refresh materialized view all_table_mv")

    var frame = sql(querySQL)
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "all_table_mv"))
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
    assert(TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "all_table_mv"))
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
      "create table mv_like(name string, age int, address string, Country string, id int) STORED AS carbondata")
    sql(
      "create materialized view mvlikedm1 as select name,address from mv_like where Country NOT LIKE 'US' group by name,address")
    sql(
      "create materialized view mvlikedm2 as select name,address,Country from mv_like where Country = 'US' or Country = 'China' group by name,address,Country")
    sql("insert into mv_like select 'chandler', 32, 'newYork', 'US', 5")
    val df1 = sql(
      "select name,address from mv_like where Country NOT LIKE 'US' group by name,address")
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "mvlikedm1"))
    val df2 = sql(
      "select name,address,Country from mv_like where Country = 'US' or Country = 'China' group by name,address,Country")
    assert(TestUtil.verifyMVHit(df2.queryExecution.optimizedPlan, "mvlikedm2"))
  }

  test("test distinct, count, sum on MV with single projection column") {
    sql("drop table if exists maintable")
    sql("create table maintable(name string, age int, add string) STORED AS carbondata")
    sql("create materialized view single_mv as select age from maintable")
    sql("insert into maintable select 'pheobe',31,'NY'")
    sql("insert into maintable select 'rachel',32,'NY'")
    val df1 = sql("select distinct(age) from maintable")
    val df2 = sql("select sum(age) from maintable")
    val df3 = sql("select count(age) from maintable")
    checkAnswer(df1, Seq(Row(31), Row(32)))
    checkAnswer(df2, Seq(Row(63)))
    checkAnswer(df3, Seq(Row(2)))
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "single_mv"))
    assert(TestUtil.verifyMVHit(df2.queryExecution.optimizedPlan, "single_mv"))
    assert(TestUtil.verifyMVHit(df3.queryExecution.optimizedPlan, "single_mv"))
  }

  test("count test case") {

    sql("drop table if exists mvtable1")
    sql("create table mvtable1(name string,age int,salary int) STORED AS carbondata")
    sql("create materialized view MV11 as select name from mvtable1")
    sql("insert into mvtable1 select 'n1',12,12")
    sql("refresh materialized view MV11")
    val frame = sql("select count(*) from mvtable1")
    assert(!TestUtil.verifyMVHit(frame.queryExecution.optimizedPlan, "MV11"))
    checkAnswer(frame, Seq(Row(1)))
    sql(s"drop materialized view MV11")
    sql("drop table if exists mvtable1")
  }

  test("test mv with duplicate columns in query and constant column") {
    // new optimized insert into flow doesn't support duplicate column names, so send it to old flow
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    sql("drop table if exists maintable")
    sql("create table maintable(name string, age int, add string) STORED AS carbondata")
    intercept[MalformedMVCommandException] {
      sql("create materialized view dupli_mv as select name, sum(age),sum(age) from maintable group by name")
    }.getMessage.contains("Cannot create mv with duplicate column: sum(maintable.age)")
    sql("create materialized view dupli_mv as select name, sum(age) from maintable group by name")
    intercept[MalformedMVCommandException] {
      sql("create materialized view dupli_projection as select age, age,add from maintable")
    }.getMessage.contains("Cannot create mv with duplicate column: maintable.age")
    sql("create materialized view dupli_projection as select age,add from maintable")
    sql("create materialized view constant_mv as select name, sum(1) ex1 from maintable group by name")
    sql("insert into maintable select 'pheobe',31,'NY'")
    val df1 = sql("select sum(age),name from maintable group by name")
    val df2 = sql("select sum(age),sum(age),name from maintable group by name")
    val df3 = sql("select name, sum(1) ex1 from maintable group by name")
    val df4 = sql("select sum(1) ex1 from maintable group by name")
    val df5 = sql("select age,age,add from maintable")
    val df6 = sql("select age,add from maintable")
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "dupli_mv"))
    assert(TestUtil.verifyMVHit(df2.queryExecution.optimizedPlan, "dupli_mv"))
    assert(TestUtil.verifyMVHit(df3.queryExecution.optimizedPlan, "constant_mv"))
    assert(TestUtil.verifyMVHit(df4.queryExecution.optimizedPlan, "constant_mv"))
    assert(TestUtil.verifyMVHit(df5.queryExecution.optimizedPlan, "dupli_projection"))
    assert(TestUtil.verifyMVHit(df6.queryExecution.optimizedPlan, "dupli_projection"))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "false")
  }

  test("test mv query when the column names and table name same in join scenario") {
    sql("drop table IF EXISTS price")
    sql("drop table IF EXISTS quality")
    sql("create table price(product string,price int) STORED AS carbondata")
    sql("create table quality(product string,quality string) STORED AS carbondata")
    sql("create materialized view same_mv as select price.product,price.price,quality.product,quality.quality from price,quality where price.product = quality.product")
    val df1 = sql("select price.product from price,quality where price.product = quality.product")
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "same_mv"))
    sql("drop materialized view if exists same_mv")
  }

  test("test materialized view column having more than 128 characters") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable (m_month smallint, c_code string, " +
        "c_country smallint, d_dollar_value double, q_quantity double, u_unit smallint, b_country smallint, i_id int, y_year smallint) STORED AS carbondata")
    sql("insert into maintable select 10, 'xxx', 123, 456, 45, 5, 23, 1, 2000")
    sql("drop materialized view if exists da_agg")
    sql("create materialized view da_agg as select u_unit, y_year, m_month, c_country, b_country, sum(case when i_id=1 and (y_year=2000 and m_month=10)" +
        "then d_dollar_value else 0 end), sum(case when i_id=1 and (y_year=2000 and m_month=10) then q_quantity else 0 end) ex, sum(case when i_id=1 and (y_year=2011 and " +
        "(m_month>=7 and m_month <=12)) then q_quantity else 0 end) from maintable group by u_unit, y_year, m_month, c_country, b_country")
    val df = sql("select u_unit, y_year, m_month, c_country, b_country, sum(case when i_id=1 and (y_year=2000 and m_month=10) then d_dollar_value else 0 end), " +
                 "sum(case when i_id=1 and (y_year=2000 and m_month=10) then q_quantity else 0 end) ex, sum(case when i_id=1 and (y_year=2011 and (m_month>=7 and m_month " +
                 "<=12)) then q_quantity else 0 end) from maintable group by u_unit,y_year, m_month, c_country, b_country")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "da_agg"))
    sql("drop materialized view if exists da_agg")
    sql("drop table IF EXISTS maintable")
  }

  test("test cast expression with mv") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable (m_month bigint, c_code string, " +
        "c_country smallint, d_dollar_value double, q_quantity double, u_unit smallint, b_country smallint, i_id int, y_year smallint) STORED AS carbondata")
    sql("insert into maintable select 10, 'xxx', 123, 456, 45, 5, 23, 1, 2000")
    sql("drop materialized view if exists da_cast")
    sql(
      "create materialized view da_cast as select cast(floor((m_month +1000) / 900) * 900 - 2000 AS INT) as a, c_code as abc,m_month from maintable")
    val df1 = sql(
      " select cast(floor((m_month +1000) / 900) * 900 - 2000 AS INT) as a ,c_code as abc  from maintable")
    val df2 = sql(
      " select cast(floor((m_month +1000) / 900) * 900 - 2000 AS INT),c_code as abc  from maintable")
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "da_cast"))
    assert(TestUtil.verifyMVHit(df2.queryExecution.optimizedPlan, "da_cast"))
  }

  test("test cast of expression with mv") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable (m_month bigint, c_code string, " +
        "c_country smallint, d_dollar_value double, q_quantity double, u_unit smallint, b_country smallint, i_id int, y_year smallint) STORED AS carbondata")
    sql("insert into maintable select 10, 'xxx', 123, 456, 45, 5, 23, 1, 2000")
    sql("drop materialized view if exists da_cast")
    sql(
      "create materialized view da_cast as select cast(floor((m_month +1000) / 900) * 900 - 2000 AS INT) as a, c_code as abc from maintable")
    val df1 = sql(
      " select cast(floor((m_month +1000) / 900) * 900 - 2000 AS INT) as a ,c_code as abc  from maintable")
    val df2 = sql(
      " select cast(floor((m_month +1000) / 900) * 900 - 2000 AS INT),c_code as abc  from maintable")
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "da_cast"))
    sql("drop materialized view if exists da_cast")
  }

  test("test cast with & without alias") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable (m_month bigint, c_code string, " +
        "c_country smallint, d_dollar_value double, q_quantity double, u_unit smallint, b_country smallint, i_id int, y_year smallint) STORED AS carbondata")
    sql("insert into maintable select 10, 'xxx', 123, 456, 45, 5, 23, 1, 2000")
    sql("drop materialized view if exists da_cast")
    sql(
      "create materialized view da_cast as select cast(m_month + 1000 AS INT) as a, c_code as abc from maintable")
    checkAnswer(sql("select cast(m_month + 1000 AS INT) as a, c_code as abc from maintable"), Seq(Row(1010, "xxx")))
    var df1 = sql("select cast(m_month + 1000 AS INT) as a, c_code as abc from maintable")
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "da_cast"))
    df1 = sql("select cast(m_month + 1000 AS INT), c_code from maintable")
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "da_cast"))
    sql("drop materialized view if exists da_cast")
    sql(
      "create materialized view da_cast as select cast(m_month + 1000 AS INT), c_code from maintable")
    df1 = sql("select cast(m_month + 1000 AS INT), c_code from maintable")
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "da_cast"))
    checkAnswer(sql("select cast(m_month + 1000 AS INT), c_code from maintable"), Seq(Row(1010, "xxx")))
    sql("drop materialized view if exists da_cast")
  }

  test("test mv with floor & ceil exp") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable (m_month bigint, c_code string, " +
        "c_country smallint, d_dollar_value double, q_quantity double, u_unit smallint, b_country smallint, i_id int, y_year smallint) STORED AS carbondata")
    sql("insert into maintable select 10, 'xxx', 123, 456, 45, 5, 23, 1, 2000")
    sql("drop materialized view if exists da_floor")
    sql(
      "create materialized view da_floor as select floor(m_month) as a, c_code as abc from maintable")
    checkAnswer(sql("select floor(m_month) as a, c_code as abc from maintable"), Seq(Row(10, "xxx")))
    val df1 = sql("select floor(m_month) as a, c_code as abc from maintable")
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "da_floor"))
    sql("drop materialized view if exists da_ceil")
    sql(
      "create materialized view da_ceil as select ceil(m_month) as a, c_code as abc from maintable")
    checkAnswer(sql("select ceil(m_month) as a, c_code as abc from maintable"), Seq(Row(10, "xxx")))
    val df2 = sql("select ceil(m_month) as a, c_code as abc from maintable")
    assert(TestUtil.verifyMVHit(df2.queryExecution.optimizedPlan, "da_ceil"))
    sql("drop materialized view if exists da_ceil")
    sql("drop materialized view if exists da_floor")
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
    sql("drop table if exists sum_agg_decimal")
  }

  test("test create materialized view with add segment") {
    sql("drop table if exists fact_table_addseg")
    sql("drop table if exists fact_table_addseg1")
    sql(
      """
        | CREATE TABLE fact_table_addseg (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table_addseg OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(
      """
        | CREATE TABLE fact_table_addseg1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table_addseg1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql("drop materialized view if exists addseg")
    sql("create materialized view addseg as select empname, designation from fact_table_addseg")
    val df = sql("select empname,designation from fact_table_addseg")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "addseg"))
    assert(df.collect().length == 90)
    val table = CarbonEnv.getCarbonTable(None, "fact_table_addseg1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "0")
    val newPath = storeLocation + "/" + "addsegtest"
    copy(path, newPath)

    sql(s"alter table fact_table_addseg add segment options('path'='$newPath', 'format'='carbon')").collect()
    sql("select empname,designation from fact_table_addseg").collect()
    val df1 = sql("select empname,designation from fact_table_addseg")
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "addseg"))
    assert(df1.collect().length == 180)
    sql(s"drop materialized view addseg")
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    sql("drop table if exists fact_table_addseg")
    sql("drop table if exists fact_table_addseg1")
  }

  test("test create materialized view with add segment with deffered refresh") {
    sql("drop table if exists fact_table_addseg")
    sql("drop table if exists fact_table_addseg1")
    sql(
      """
        | CREATE TABLE fact_table_addseg (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table_addseg OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(
      """
        | CREATE TABLE fact_table_addseg1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table_addseg1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql("drop materialized view if exists addseg")
    sql("create materialized view addseg with deferred refresh as select empname, designation from fact_table_addseg")
    sql("refresh materialized view addseg")
    val df = sql("select empname,designation from fact_table_addseg")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "addseg"))
    assert(df.collect().length == 90)
    val table = CarbonEnv.getCarbonTable(None, "fact_table_addseg1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "0")
    val newPath = storeLocation + "/" + "addsegtest"
    copy(path, newPath)

    sql(s"alter table fact_table_addseg add segment options('path'='$newPath', 'format'='carbon')").collect()
    val df1 = sql("select empname,designation from fact_table_addseg")
    assert(!TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "addseg"))
    assert(df1.collect().length == 180)

    sql("refresh materialized view addseg")

    val df2 = sql("select empname,designation from fact_table_addseg")
    assert(TestUtil.verifyMVHit(df2.queryExecution.optimizedPlan, "addseg"))
    assert(df2.collect().length == 180)

    sql("drop materialized view addseg")
    sql("drop table if exists fact_table_addseg")
    sql("drop table if exists fact_table_addseg1")
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("test join query with & without filter columns in projection") {
    sql("drop table if exists t1")
    sql("drop table if exists t2")
    sql("drop materialized view if exists mv1")
    sql("create table t1(userId string,score int) STORED AS carbondata")
    sql("create table t2(userId string,age int,sex string) STORED AS carbondata")
    sql("insert into t1 values(1,100),(2,500)")
    sql("insert into t2 values(1,20,'f'),(2,30,'m')")
    val result = sql("select avg(t1.score),t2.age,t2.sex from t1 join t2 on t1.userId=t2.userId group by t2.age,t2.sex")
    sql("create materialized view mv1 as select avg(t1.score),t2.age,t2.sex from t1 join t2 on t1.userId=t2.userId group by t2.age,t2.sex")
    val df = sql("select avg(t1.score),t2.age,t2.sex from t1 join t2 on t1.userId=t2.userId group by t2.age,t2.sex")
    TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1")
    checkAnswer(df, result)
    intercept[ProcessMetaDataException] {
      sql("alter table t1 drop columns(userId)")
    }.getMessage.contains("Column name cannot be dropped because it exists in mv materialized view: mv1")
    sql("drop table if exists t1")
    sql("drop table if exists t2")
  }

  test("test sum aggregations on decimal columns") {
    sql("drop table if exists sum_agg_decimal")
    sql("create table sum_agg_decimal(salary1 decimal(7,2),salary2 decimal(7,2),salary3 decimal(7,2),salary4 decimal(7,2),empname string) stored as carbondata")
    sql("drop materialized view if exists decimal_mv")
    sql("create materialized view decimal_mv as select empname, sum(salary1 - salary2) from sum_agg_decimal group by empname")
    val df = sql("select empname, sum( salary1 - salary2) from sum_agg_decimal group by empname")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "decimal_mv"))
  }

  def copy(oldLoc: String, newLoc: String): Unit = {
    val oldFolder = FileFactory.getCarbonFile(oldLoc)
    FileFactory.mkdirs(newLoc, FileFactory.getConfiguration)
    val oldFiles = oldFolder.listFiles
    for (file <- oldFiles) {
      Files.copy(Paths.get(file.getParentFile.getPath, file.getName),
        Paths.get(newLoc, file.getName))
    }
  }

  override def afterAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }
  // scalastyle:on lineLength
}

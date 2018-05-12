package org.apache.carbondata.mv.rewrite

import java.io.File

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class MVCreateTestCase extends QueryTest with BeforeAndAfterAll {

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
    sql(s"rebuild datamap datamap1")
    val df = sql("select empname,designation from fact_table1")
    val analyzed = df.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap1"))
    checkAnswer(df, sql("select empname,designation from fact_table2"))
    sql(s"drop datamap datamap1")
  }

  test("test create datamap with simple and sub projection") {
    sql("drop datamap if exists datamap2")
    sql("create datamap datamap2 using 'mv' as select empname, designation from fact_table1")
    sql(s"rebuild datamap datamap2")
    val df = sql("select empname from fact_table1")
    val analyzed = df.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap2"))
    checkAnswer(df, sql("select empname from fact_table2"))
    sql(s"drop datamap datamap2")
  }

  test("test create datamap with simple and same projection with projection filter") {
    sql("drop datamap if exists datamap3")
    sql("create datamap datamap3 using 'mv' as select empname, designation from fact_table1")
    sql(s"rebuild datamap datamap3")
    val frame = sql("select empname, designation from fact_table1 where empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap3"))

    checkAnswer(frame, sql("select empname, designation from fact_table2 where empname='shivani'"))
    sql(s"drop datamap datamap3")
  }

  test("test create datamap with simple and sub projection with non projection filter") {
    sql("create datamap datamap4 using 'mv' as select empname, designation from fact_table1")
    sql(s"rebuild datamap datamap4")
    val frame = sql("select designation from fact_table1 where empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap4"))
    checkAnswer(frame, sql("select designation from fact_table2 where empname='shivani'"))
    sql(s"drop datamap datamap4")
  }

  test("test create datamap with simple and sub projection with datamap filter") {
    sql("create datamap datamap5 using 'mv' as select empname, designation from fact_table1 where empname='shivani'")
    sql(s"rebuild datamap datamap5")
    val frame = sql("select designation from fact_table1 where empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap5"))
    checkAnswer(frame, sql("select designation from fact_table2 where empname='shivani'"))
    sql(s"drop datamap datamap5")
  }

  test("test create datamap with simple and same projection with datamap filter ") {
    sql("create datamap datamap6 using 'mv' as select empname, designation from fact_table1 where empname='shivani'")
    sql(s"rebuild datamap datamap6")
    val frame = sql("select empname,designation from fact_table1 where empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap6"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where empname='shivani'"))
    sql(s"drop datamap datamap6")
  }

  test("test create datamap with simple and same projection with datamap filter and extra query column filter") {
    sql("create datamap datamap7 using 'mv' as select empname, designation from fact_table1 where empname='shivani'")
    sql(s"rebuild datamap datamap7")
    val frame = sql(
      "select empname,designation from fact_table1 where empname='shivani' and designation='SA'")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap7"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where empname='shivani' and designation='SA'"))
    sql(s"drop datamap datamap7")
  }

  test("test create datamap with simple and same projection with datamap filter and different column filter") {
    sql("create datamap datamap8 using 'mv' as select empname, designation from fact_table1 where empname='shivani'")
    sql(s"rebuild datamap datamap8")
    val frame = sql("select empname,designation from fact_table1 where designation='SA'")
    val analyzed = frame.queryExecution.analyzed
    assert(!verifyMVDataMap(analyzed, "datamap8"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where designation='SA'"))
    sql(s"drop datamap datamap8")
  }

  test("test create datamap with simple and same projection with datamap filter on non projection column and extra column filter") {
    sql("create datamap datamap9 using 'mv' as select empname, designation from fact_table1 where deptname='cloud'")
    sql(s"rebuild datamap datamap9")
    val frame = sql("select empname,designation from fact_table1 where deptname='cloud'")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap9"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where deptname='cloud'"))
    sql(s"drop datamap datamap9")
  }

  test("test create datamap with simple and same projection with datamap filter on non projection column and no column filter") {
    sql("create datamap datamap10 using 'mv' as select empname, designation from fact_table1 where deptname='cloud'")
    sql(s"rebuild datamap datamap10")
    val frame = sql("select empname,designation from fact_table1")
    val analyzed = frame.queryExecution.analyzed
    assert(!verifyMVDataMap(analyzed, "datamap10"))
    checkAnswer(frame, sql("select empname,designation from fact_table2"))
    sql(s"drop datamap datamap10")
  }

  test("test create datamap with simple and same projection with datamap filter on non projection column and different column filter") {
    sql("create datamap datamap11 using 'mv' as select empname, designation from fact_table1 where deptname='cloud'")
    sql(s"rebuild datamap datamap11")
    val frame = sql("select empname,designation from fact_table1 where designation='SA'")
    val analyzed = frame.queryExecution.analyzed
    assert(!verifyMVDataMap(analyzed, "datamap11"))
    checkAnswer(frame, sql("select empname,designation from fact_table2 where designation='SA'"))
    sql(s"drop datamap datamap11")
  }

  test("test create datamap with simple and same group by query") {
    sql("drop datamap if exists datamap12")
    sql("create datamap datamap12 using 'mv' as select empname, sum(utilization) from fact_table1 group by empname")
    sql(s"rebuild datamap datamap12")
    val frame = sql("select empname, sum(utilization) from fact_table1 group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap12"))
    checkAnswer(frame, sql("select empname, sum(utilization) from fact_table2 group by empname"))
    sql(s"drop datamap datamap12")
  }

  test("test create datamap with simple and sub group by query") {
    sql("drop datamap if exists datamap13")
    sql("create datamap datamap13 using 'mv' as select empname, sum(utilization) from fact_table1 group by empname")
    sql(s"rebuild datamap datamap13")
    val frame = sql("select sum(utilization) from fact_table1 group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap13"))
    checkAnswer(frame, sql("select sum(utilization) from fact_table2 group by empname"))
    sql(s"drop datamap datamap13")
  }

  test("test create datamap with simple and sub group by query with filter on query") {
    sql("drop datamap if exists datamap14")
    sql("create datamap datamap14 using 'mv' as select empname, sum(utilization) from fact_table1 group by empname")
    sql(s"rebuild datamap datamap14")
    val frame = sql(
      "select empname,sum(utilization) from fact_table1 group by empname having empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap14"))
    checkAnswer(frame, sql("select empname,sum(utilization) from fact_table2 where empname='shivani' group by empname"))
    sql(s"drop datamap datamap14")
  }

  test("test create datamap with simple and sub group and sub projection by query with filter on query") {
    sql("drop datamap if exists datamap32")
    sql("create datamap datamap32 using 'mv' as select empname, sum(utilization) from fact_table1 group by empname")
    sql(s"rebuild datamap datamap32")
    val frame = sql(
      "select empname, sum(utilization) from fact_table1 group by empname having empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap32"))
    checkAnswer(frame, sql( "select empname, sum(utilization) from fact_table2 group by empname having empname='shivani'"))
    sql(s"drop datamap datamap32")
  }

  test("test create datamap with simple and sub group by query with filter on datamap") {
    sql("create datamap datamap15 using 'mv' as select empname, sum(utilization) from fact_table1 where empname='shivani' group by empname")
    sql(s"rebuild datamap datamap15")
    val frame = sql(
      "select empname,sum(utilization) from fact_table1 where empname='shivani' group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap15"))
    checkAnswer(frame, sql("select empname,sum(utilization) from fact_table2 where empname='shivani' group by empname"))
    sql(s"drop datamap datamap15")
  }

  test("test create datamap with simple and sub group by query with filter on datamap and no filter on query") {
    sql("create datamap datamap16 using 'mv' as select empname, sum(utilization) from fact_table1 where empname='shivani' group by empname")
    sql(s"rebuild datamap datamap16")
    val frame = sql("select empname,sum(utilization) from fact_table1 group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(!verifyMVDataMap(analyzed, "datamap16"))
    checkAnswer(frame, sql("select empname,sum(utilization) from fact_table2 group by empname"))
    sql(s"drop datamap datamap16")
  }

  test("test create datamap with simple and same group by with expression") {
    sql("create datamap datamap17 using 'mv' as select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    sql(s"rebuild datamap datamap17")
    val frame = sql(
      "select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group" +
      " by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap17"))
    checkAnswer(frame, sql("select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table2 group" +
                           " by empname"))
    sql(s"drop datamap datamap17")
  }

  test("test create datamap with simple and sub group by with expression") {
    sql("drop datamap if exists datamap18")
    sql("create datamap datamap18 using 'mv' as select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    sql(s"rebuild datamap datamap18")
    val frame = sql(
      "select sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap18"))
    checkAnswer(frame, sql("select sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table2 group by empname"))
    sql(s"drop datamap datamap18")
  }

  test("test create datamap with simple and sub count group by with expression") {
    sql("drop datamap if exists datamap19")
    sql("create datamap datamap19 using 'mv' as select empname, count(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    sql(s"rebuild datamap datamap19")
    val frame = sql(
      "select count(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap19"))
    checkAnswer(frame, sql("select count(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table2 group by empname"))
    sql(s"drop datamap datamap19")
  }

  test("test create datamap with simple and sub group by with expression and filter on query") {
    sql("drop datamap if exists datamap20")
    sql("create datamap datamap20 using 'mv' as select empname, sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 group by empname")
    sql(s"rebuild datamap datamap20")
    val frame = sql(
      "select sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table1 where " +
      "empname='shivani' group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap20"))
    checkAnswer(frame, sql("select sum(CASE WHEN utilization=27 THEN deptno ELSE 0 END) from fact_table2 where " +
                           "empname='shivani' group by empname"))
    sql(s"drop datamap datamap20")
  }

  test("test create datamap with simple join") {
    sql("drop datamap if exists datamap21")
    sql("create datamap datamap21 using 'mv' as select t1.empname as c1, t2.designation, t2.empname as c2 from fact_table1 t1,fact_table2 t2 where t1.empname = t2.empname")
    sql(s"rebuild datamap datamap21")
    val frame = sql(
      "select t1.empname as c1, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = t2.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap21"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = t2.empname"))
    sql(s"drop datamap datamap21")
  }

  test("test create datamap with simple join and filter on query") {
    sql("drop datamap if exists datamap22")
    sql("create datamap datamap22 using 'mv' as select t1.empname, t2.designation, t2.empname from fact_table1 t1,fact_table2 t2 where t1.empname = t2.empname")
    sql(s"rebuild datamap datamap22")
    val frame = sql(
      "select t1.empname, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = " +
      "t2.empname and t1.empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap22"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = " +
                           "t2.empname and t1.empname='shivani'"))
    sql(s"drop datamap datamap22")
  }


  test("test create datamap with simple join and filter on query and datamap") {
    sql("drop datamap if exists datamap23")
    sql("create datamap datamap23 using 'mv' as select t1.empname, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = t2.empname and t1.empname='shivani'")
    sql(s"rebuild datamap datamap23")
    val frame = sql(
      "select t1.empname, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = " +
      "t2.empname and t1.empname='shivani'")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap23"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = " +
                           "t2.empname and t1.empname='shivani'"))
    sql(s"drop datamap datamap23")
  }

  test("test create datamap with simple join and filter on datamap and no filter on query") {
    sql("drop datamap if exists datamap24")
    sql("create datamap datamap24 using 'mv' as select t1.empname, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = t2.empname and t1.empname='shivani'")
    sql(s"rebuild datamap datamap24")
    val frame = sql(
      "select t1.empname, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = t2.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(!verifyMVDataMap(analyzed, "datamap24"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = t2.empname"))
    sql(s"drop datamap datamap24")
  }

  test("test create datamap with multiple join") {
    sql("drop datamap if exists datamap25")
    sql("create datamap datamap25 using 'mv' as select t1.empname as c1, t2.designation from fact_table1 t1,fact_table2 t2,fact_table3 t3  where t1.empname = t2.empname and t1.empname=t3.empname")
    sql(s"rebuild datamap datamap25")
    val frame = sql(
      "select t1.empname as c1, t2.designation from fact_table1 t1,fact_table2 t2 where t1.empname = t2.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(!verifyMVDataMap(analyzed, "datamap25"))
    checkAnswer(frame, sql("select t1.empname, t2.designation from fact_table4 t1,fact_table5 t2 where t1.empname = t2.empname"))
    sql(s"drop datamap datamap25")
  }

  ignore("test create datamap with simple join on datamap and multi join on query") {
    sql("create datamap datamap26 using 'mv' as select t1.empname, t2.designation, t2.empname from fact_table1 t1,fact_table2 t2 where t1.empname = t2.empname")
    sql(s"rebuild datamap datamap26")
    val frame = sql(
      "select t1.empname, t2.designation, t2.empname from fact_table1 t1,fact_table2 t2,fact_table3 " +
      "t3  where t1.empname = t2.empname and t1.empname=t3.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap26"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, t2.empname from fact_table4 t1,fact_table5 t2,fact_table6 " +
                           "t3  where t1.empname = t2.empname and t1.empname=t3.empname"))
    sql(s"drop datamap datamap26")
  }

  test("test create datamap with join with group by") {
    sql("create datamap datamap27 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where t1.empname = t2.empname group by t1.empname, t2.designation")
    sql(s"rebuild datamap datamap27")
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname group by t1.empname, t2.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap27"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  " +
                           "where t1.empname = t2.empname group by t1.empname, t2.designation"))
    sql(s"drop datamap datamap27")
  }

  test("test create datamap with join with group by and sub projection") {
    sql("drop datamap if exists datamap28")
    sql("create datamap datamap28 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where t1.empname = t2.empname group by t1.empname, t2.designation")
    sql(s"rebuild datamap datamap28")
    val frame = sql(
      "select t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where " +
      "t1.empname = t2.empname group by t2.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap28"))
    checkAnswer(frame, sql("select t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  where " +
                           "t1.empname = t2.empname group by t2.designation"))
    sql(s"drop datamap datamap28")
  }

  test("test create datamap with join with group by and sub projection with filter") {
    sql("drop datamap if exists datamap29")
    sql("create datamap datamap29 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where t1.empname = t2.empname group by t1.empname, t2.designation")
    sql(s"rebuild datamap datamap29")
    val frame = sql(
      "select t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where " +
      "t1.empname = t2.empname and t1.empname='shivani' group by t2.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap29"))
    checkAnswer(frame, sql("select t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  where " +
                           "t1.empname = t2.empname and t1.empname='shivani' group by t2.designation"))
    sql(s"drop datamap datamap29")
  }

  test("test create datamap with join with group by with filter") {
    sql("drop datamap if exists datamap30")
    sql("create datamap datamap30 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  where t1.empname = t2.empname group by t1.empname, t2.designation")
    sql(s"rebuild datamap datamap30")
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname and t2.designation='SA' group by t1.empname, t2.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap30"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1,fact_table5 t2  " +
                           "where t1.empname = t2.empname and t2.designation='SA' group by t1.empname, t2.designation"))
    sql(s"drop datamap datamap30")
  }

  test("test create datamap with expression on projection") {
    sql(s"drop datamap if exists datamap31")
    sql("create datamap datamap31 using 'mv' as select empname, designation, utilization, projectcode from fact_table1 ")
    sql(s"rebuild datamap datamap31")
    val frame = sql(
      "select empname, designation, utilization+projectcode from fact_table1")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap31"))
    checkAnswer(frame, sql("select empname, designation, utilization+projectcode from fact_table2"))
    sql(s"drop datamap datamap31")
  }

  test("test create datamap with simple and sub group by query and count agg") {
    sql(s"drop datamap if exists datamap32")
    sql("create datamap datamap32 using 'mv' as select empname, count(utilization) from fact_table1 group by empname")
    sql(s"rebuild datamap datamap32")
    val frame = sql("select empname,count(utilization) from fact_table1 where empname='shivani' group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap32"))
    checkAnswer(frame, sql("select empname,count(utilization) from fact_table2 where empname='shivani' group by empname"))
    sql(s"drop datamap datamap32")
  }

  ignore("test create datamap with simple and sub group by query and avg agg") {
    sql(s"drop datamap if exists datamap33")
    sql("create datamap datamap33 using 'mv' as select empname, avg(utilization) from fact_table1 group by empname")
    sql(s"rebuild datamap datamap33")
    val frame = sql("select empname,avg(utilization) from fact_table1 where empname='shivani' group by empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap33"))
    checkAnswer(frame, sql("select empname,avg(utilization) from fact_table2 where empname='shivani' group by empname"))
    sql(s"drop datamap datamap33")
  }

  test("test create datamap with left join with group by") {
    sql("drop datamap if exists datamap34")
    sql("create datamap datamap34 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    sql(s"rebuild datamap datamap34")
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname group by t1.empname, t2.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap34"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname group by t1.empname, t2.designation"))
    sql(s"drop datamap datamap34")
  }

  test("test create datamap with simple and group by query with filter on datamap but not on projection") {
    sql("create datamap datamap35 using 'mv' as select designation, sum(utilization) from fact_table1 where empname='shivani' group by designation")
    sql(s"rebuild datamap datamap35")
    val frame = sql(
      "select designation, sum(utilization) from fact_table1 where empname='shivani' group by designation")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap35"))
    checkAnswer(frame, sql("select designation, sum(utilization) from fact_table2 where empname='shivani' group by designation"))
    sql(s"drop datamap datamap35")
  }

  test("test create datamap with simple and sub group by query with filter on datamap but not on projection") {
    sql("create datamap datamap36 using 'mv' as select designation, sum(utilization) from fact_table1 where empname='shivani' group by designation")
    sql(s"rebuild datamap datamap36")
    val frame = sql(
      "select sum(utilization) from fact_table1 where empname='shivani' group by designation")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap36"))
    checkAnswer(frame, sql("select sum(utilization) from fact_table2 where empname='shivani' group by designation"))
    sql(s"drop datamap datamap36")
  }

  test("test create datamap with agg push join with sub group by ") {
    sql("drop datamap if exists datamap37")
    sql("create datamap datamap37 using 'mv' as select empname, designation, sum(utilization) from fact_table1 group by empname, designation")
    sql(s"rebuild datamap datamap37")
    val frame = sql(
      "select t1.empname, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname group by t1.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap37"))
    checkAnswer(frame, sql("select t1.empname, sum(t1.utilization) from fact_table3 t1,fact_table4 t2  " +
                           "where t1.empname = t2.empname group by t1.empname, t1.designation"))
    sql(s"drop datamap datamap37")
  }

  test("test create datamap with agg push join with group by ") {
    sql("drop datamap if exists datamap38")
    sql("create datamap datamap38 using 'mv' as select empname, designation, sum(utilization) from fact_table1 group by empname, designation")
    sql(s"rebuild datamap datamap38")
    val frame = sql(
      "select t1.empname, t1.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname group by t1.empname,t1.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap38"))
    checkAnswer(frame, sql("select t1.empname,t1.designation, sum(t1.utilization) from fact_table3 t1,fact_table4 t2  " +
                           "where t1.empname = t2.empname group by t1.empname, t1.designation"))
    sql(s"drop datamap datamap38")
  }

  test("test create datamap with agg push join with group by with filter") {
    sql("drop datamap if exists datamap39")
    sql("create datamap datamap39 using 'mv' as select empname, designation, sum(utilization) from fact_table1 group by empname, designation ")
    sql(s"rebuild datamap datamap39")
    val frame = sql(
      "select t1.empname, t1.designation, sum(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname and t1.empname='shivani' group by t1.empname,t1.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap39"))
    checkAnswer(frame, sql("select t1.empname,t1.designation, sum(t1.utilization) from fact_table3 t1,fact_table4 t2  " +
                           "where t1.empname = t2.empname and t1.empname='shivani' group by t1.empname, t1.designation"))
    sql(s"drop datamap datamap39")
  }

  test("test create datamap with more agg push join with group by with filter") {
    sql("drop datamap if exists datamap40")
    sql("create datamap datamap40 using 'mv' as select empname, designation, sum(utilization), count(utilization) from fact_table1 group by empname, designation ")
    sql(s"rebuild datamap datamap40")
    val frame = sql(
      "select t1.empname, t1.designation, sum(t1.utilization),count(t1.utilization) from fact_table1 t1,fact_table2 t2  " +
      "where t1.empname = t2.empname and t1.empname='shivani' group by t1.empname,t1.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap40"))
    checkAnswer(frame, sql("select t1.empname, t1.designation, sum(t1.utilization),count(t1.utilization) from fact_table3 t1,fact_table4 t2  " +
                           "where t1.empname = t2.empname and t1.empname='shivani' group by t1.empname,t1.designation"))
    sql(s"drop datamap datamap40")
  }

  test("test create datamap with left join with group by with filter") {
    sql("drop datamap if exists datamap41")
    sql("create datamap datamap41 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    sql(s"rebuild datamap datamap41")
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname, t2.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap41"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname, t2.designation"))
    sql(s"drop datamap datamap41")
  }

  test("test create datamap with left join with sub group by") {
    sql("drop datamap if exists datamap42")
    sql("create datamap datamap42 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    sql(s"rebuild datamap datamap42")
    val frame = sql(
      "select t1.empname, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname group by t1.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap42"))
    checkAnswer(frame, sql("select t1.empname, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname group by t1.empname"))
    sql(s"drop datamap datamap42")
  }

  test("test create datamap with left join with sub group by with filter") {
    sql("drop datamap if exists datamap43")
    sql("create datamap datamap43 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    sql(s"rebuild datamap datamap43")
    val frame = sql(
      "select t1.empname, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap43"))
    checkAnswer(frame, sql("select t1.empname, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname"))
    sql(s"drop datamap datamap43")
  }

  test("test create datamap with left join with sub group by with filter on mv") {
    sql("drop datamap if exists datamap44")
    sql("create datamap datamap44 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname, t2.designation")
    sql(s"rebuild datamap datamap44")
    val frame = sql(
      "select t1.empname, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap44"))
    checkAnswer(frame, sql("select t1.empname, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname where t1.empname='shivani' group by t1.empname"))
    sql(s"drop datamap datamap44")
  }

  test("test create datamap with left join on query and equi join on mv with group by with filter") {
    sql("drop datamap if exists datamap45")

    sql("create datamap datamap45 using 'mv' as select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 join fact_table2 t2  on t1.empname = t2.empname group by t1.empname, t2.designation")
    sql(s"rebuild datamap datamap45")
    // During spark optimizer it converts the left outer join queries with equi join if any filter present on right side table
    val frame = sql(
      "select t1.empname, t2.designation, sum(t1.utilization) from fact_table1 t1 left join fact_table2 t2  " +
      "on t1.empname = t2.empname where t2.designation='SA' group by t1.empname, t2.designation")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap45"))
    checkAnswer(frame, sql("select t1.empname, t2.designation, sum(t1.utilization) from fact_table4 t1 left join fact_table5 t2  " +
                           "on t1.empname = t2.empname where t2.designation='SA' group by t1.empname, t2.designation"))
    sql(s"drop datamap datamap45")
  }


  def verifyMVDataMap(logicalPlan: LogicalPlan, dataMapName: String): Boolean = {
    val tables = logicalPlan collect {
      case l: LogicalRelation => l.catalogTable.get
    }
    tables.exists(_.identifier.table.equalsIgnoreCase(dataMapName+"_table"))
  }


  def drop(): Unit = {
    sql("drop table IF EXISTS fact_table1")
    sql("drop table IF EXISTS fact_table2")
    sql("drop table IF EXISTS fact_table3")
    sql("drop table IF EXISTS fact_table4")
    sql("drop table IF EXISTS fact_table5")
    sql("drop table IF EXISTS fact_table6")
  }

  override def afterAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }
}

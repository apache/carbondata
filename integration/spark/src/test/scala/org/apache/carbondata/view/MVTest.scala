package org.apache.carbondata.view

import java.io.File

import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class MVTest extends QueryTest with BeforeAndAfterAll {

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
        | CREATE TABLE fact_table (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
  }

  test("test create mv on hive table") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("create table source as select * from fact_table")
    sql("create materialized view mv1 as select empname, deptname, avg(salary) from source group by empname, deptname")
    val df = sql("select empname, avg(salary) from source group by empname")
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table group by empname"))
    sql(s"drop materialized view mv1")
    sql("drop table source")
  }

  test("test create mv on orc table") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("create table source using orc as select * from fact_table")
    sql("create materialized view mv1 as select empname, deptname, avg(salary) from source group by empname, deptname")
    val df = sql("select empname, avg(salary) from source group by empname")
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table group by empname"))
    sql(s"drop materialized view mv1")
    sql("drop table source")
  }

  test("test create mv on parquet table") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("create table source using parquet as select * from fact_table")
    sql("create materialized view mv1 as select empname, deptname, avg(salary) from source group by empname, deptname")
    val df = sql("select empname, avg(salary) from source group by empname")
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table group by empname"))
    sql(s"drop materialized view mv1")
    sql("drop table source")
  }

  test("test create mv on carbon table") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("create table source using carbondata as select * from fact_table")
    sql("create materialized view mv1 as select empname, deptname, avg(salary) from source group by empname, deptname")
    val df = sql("select empname, avg(salary) from source group by empname")
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, sql("select empname, avg(salary) from fact_table group by empname"))
    sql(s"drop materialized view mv1")
    sql("drop table source")
  }

  test("test create mv fail because of name used") {
    sql("drop table if exists mv1")
    sql("drop materialized view if exists mv1")
    sql("create table mv1 using orc as select * from fact_table")
    try {
      sql(
        "create materialized view mv1 as select empname, deptname, avg(salary) from fact_table group by empname, deptname")

    } catch {
      case _: TableAlreadyExistsException => // Expected
    } finally {
      sql("drop table if exists mv1")
      sql("drop materialized view if exists mv1")
    }
  }

  test("test drop mv") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("create table source using carbondata as select * from fact_table")
    try {
      sql(
        "create materialized view mv1 as select empname, deptname, avg(salary) from source group by empname, deptname")
      try {
        sql("drop table mv1")
      } catch {
        case _: ProcessMetaDataException => // Expected
      }
      sql("drop materialized view mv1")
    } finally {
      sql("drop table source")
    }
  }

  test("test refresh mv on manual") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("drop table if exists fact_table_compare")
    sql("create table fact_table_compare using carbondata as select * from fact_table")
    sql("create table source using carbondata as select * from fact_table")
    try {
      val ctasQuery = "select empname, deptname, count(salary) from source group by empname, deptname"
      val testQuery = "select empname, count(salary) from source group by empname"
      val compareQuery = "select empname, count(salary) from fact_table_compare group by empname"
      sql(s"create materialized view mv1 properties('refresh_trigger_mode'='on_manual') as $ctasQuery")
      var df = sql(testQuery)
      assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
      checkAnswer(df, sql(compareQuery))
      sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE source OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
      df = sql(testQuery)
      assert(!isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
      sql("refresh materialized view mv1")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table_compare OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
      df = sql(testQuery)
      assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
      checkAnswer(df, sql(compareQuery))
    } finally {
      sql("drop materialized view mv1")
      sql("drop table fact_table_compare")
      sql("drop table source")
    }
  }

  test("test incremental refresh mv") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("drop table if exists fact_table_compare")
    sql("create table fact_table_compare using carbondata as select * from fact_table")
    sql("create table source using carbondata as select * from fact_table")
    try {
      val query = "select empname, deptname, salary from source where empname='arvind'"
      val compareQuery = "select empname, deptname, salary from fact_table_compare where empname='arvind'"
      sql(s"create materialized view mv1 as $query")
      var df = sql(query)
      assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
      checkAnswer(df, sql(compareQuery))
      sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table_compare OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE source OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
      df = sql(query)
      assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
      checkAnswer(df, sql(compareQuery))
    } finally {
      sql("drop table fact_table_compare")
      sql("drop materialized view mv1")
      sql("drop table source")
    }
  }

  override def afterAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }

  def drop(): Unit = {
    sql("drop table IF EXISTS fact_table")
    sql("drop table IF EXISTS fact_table")
  }

  def isTableAppearedInPlan(logicalPlan: LogicalPlan, tableName: String): Boolean = {
    val tables = logicalPlan collect {
      case relation: LogicalRelation => relation.catalogTable.get
      case relation: HiveTableRelation => relation.tableMeta
    }
    tables.exists(_.identifier.table.equalsIgnoreCase(tableName))
  }

}

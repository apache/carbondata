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
package org.apache.carbondata.view

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedMVCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class MVTest extends QueryTest with BeforeAndAfterAll {
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
        | CREATE TABLE fact_table (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql("set carbon.enable.mv = true")
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

  test("test disable mv with carbonproperties and sessionparam") {
    // 1. Prepare the source table and MV, make sure the MV is enabled
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("create table source as select * from fact_table")
    sql("create materialized view mv1 as select empname, deptname, avg(salary) from source group by empname, deptname")
    var df = sql("select empname, avg(salary) from source group by empname")
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))

    // 2.  test disable mv with carbon.properties
    // 2.1 disable MV when set carbon.enable.mv = false in the carbonproperties
    sql("set carbon.enable.mv = false")
    df = sql("select empname, avg(salary) from source group by empname")
    assert(!isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
    // 2.2 enable MV when configuared value is invalid
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_MV, "invalidvalue")
    df = sql("select empname, avg(salary) from source group by empname")
    assert(!isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))

    // 2.3 enable mv when set carbon.enable.mv = true in the carbonproperties
    sql("set carbon.enable.mv = true")
    df = sql("select empname, avg(salary) from source group by empname")
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))

    // 3.  test disable mv with sessionparam
    // 3.1 disable MV when set carbon.enable.mv = false in the sessionparam
    sql("set carbon.enable.mv = false")
    df = sql("select empname, avg(salary) from source group by empname")
    assert(!isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))

    // 3.2 validate configuared sessionparam
    val exMessage = intercept[Exception] {
      sql("set carbon.enable.mv = invalidvalue")
    }
    assert(exMessage.getMessage.contains("Invalid value invalidvalue for key carbon.enable.mv"))

    // 3.3 enable mv when set carbon.enable.mv = true in the sessionparam
    sql("set carbon.enable.mv = true")
    df = sql("select empname, avg(salary) from source group by empname")
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
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

  test("test create mv on carbon table with avg aggregate") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("create table source(empname string, salary long) stored as carbondata")
    sql("insert into source select 'sd',20")
    sql("insert into source select 'sd',200")
    sql("create materialized view mv1 as select empname, avg(salary) from source group by empname")
    sql("insert into source select 'sd',30")
    val result = sql("show materialized views on table source").collectAsList()
    assert(result.get(0).get(3).toString.equalsIgnoreCase("incremental"))
    val df = sql("select empname, avg(salary) from source group by empname")
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, Seq(Row("sd", 83.33334)))
    sql(s"drop materialized view mv1")
    sql("drop table source")
  }

  test("test create mv with avg on carbon partition table") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("create table source(a string, empname string) stored as carbondata partitioned by(salary long)")
    sql("insert into source select 'sd','sd',20")
    sql("insert into source select 'sdf','sd',200")
    sql("create materialized view mv1 as select empname, avg(salary) from source group by empname")
    sql("insert into source select 'dsf','sd',30")
    val result = sql("show materialized views on table source").collectAsList()
    assert(result.get(0).get(3).toString.equalsIgnoreCase("incremental"))
    val df = sql("select empname, avg(salary) from source group by empname")
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, Seq(Row("sd", 83.33334)))
    sql(s"drop materialized view mv1")
    sql("drop table source")
  }

  test("test create mv with avg and compaction") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("create table source(a string, empname string, salary long) stored as carbondata")
    sql("insert into source select 'sd','sd',20")
    sql("insert into source select 'sdf','sd',200")
    sql("create materialized view mv1 as select empname, avg(salary) from source group by empname")
    sql("insert into source select 'dsf','sd',30")
    sql("insert into source select 'dsf','sd',10")
    sql("alter table source compact 'minor'")
    val result = sql("show materialized views on table source").collectAsList()
    assert(result.get(0).get(3).toString.equalsIgnoreCase("incremental"))
    val df = sql("select empname, avg(salary) from source group by empname")
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, Seq(Row("sd", 65)))
    sql(s"drop materialized view mv1")
    sql("drop table source")
  }

  test("test create MV with average of floor") {
    sql("drop table if exists source")
    sql(
      "create table if not exists source (tags_id STRING, value DOUBLE) stored as carbondata")
    sql("insert into source values ('xyz-e01',3.34)")
    sql("insert into source values ('xyz-e01',1.25)")
    val mvQuery = "select tags_id, avg(floor(value)) from source group by tags_id"
    sql("drop materialized view if exists dm1")
    sql(s"create materialized view dm1  as $mvQuery")
    sql("insert into source values ('xyz-e01',3.54)")
    val df = sql("select tags_id, avg(floor(value)) as sum_val from source group by tags_id")
    val result = sql("show materialized views on table source").collectAsList()
    assert(result.get(0).get(3).toString.equalsIgnoreCase("incremental"))
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "dm1"))
    checkAnswer(df, Seq(Row("xyz-e01", 2.33334)))
    sql("drop materialized view if exists dm1")
    sql("drop table if exists source")
  }

  test("test create mv on carbon table with avg inside other function") {
    sql("drop materialized view if exists mv1")
    sql("drop table if exists source")
    sql("create table source(a string, empname string, salary long) stored as carbondata")
    sql("insert into source select 'sd','sd',20")
    sql("insert into source select 'sdf','sd',200")
    sql("create materialized view mv1 as select empname,round(avg(salary),0) from source group by empname")
    sql("insert into source select 'dsf','sd',30")
    val df = sql("select empname, round(avg(salary),0) from source group by empname")
    val result = sql("show materialized views on table source").collectAsList()
    assert(result.get(0).get(3).toString.equalsIgnoreCase("full"))
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(df, Seq(Row("sd", 83)))
    sql(s"drop materialized view mv1")
    sql("drop table source")
  }

  test("test average MV with sum and count columns") {
    sql("drop table if exists source")
    sql("create table if not exists source (tags_id STRING, value DOUBLE) stored as carbondata")
    sql("insert into source values ('xyz-e01',3)")
    sql("insert into source values ('xyz-e01',1)")
    var mvQuery = "select tags_id ,sum(value), avg(value) from source group by tags_id"
    sql("drop materialized view if exists dm1")
    sql(s"create materialized view dm1  as $mvQuery")
    var result = sql("show materialized views on table source").collectAsList()
    assert(result.get(0).get(3).toString.equalsIgnoreCase("incremental"))
    sql("insert into source values ('xyz-e01',3)")
    var df = sql(mvQuery)
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "dm1"))
    checkAnswer(df, Seq(Row("xyz-e01", 7, 2.33334)))

    mvQuery = "select tags_id ,sum(value), avg(value),count(value) from source group by tags_id"
    sql("drop materialized view if exists dm1")
    sql(s"create materialized view dm1  as $mvQuery")
    result = sql("show materialized views on table source").collectAsList()
    assert(result.get(0).get(3).toString.equalsIgnoreCase("incremental"))
    sql("insert into source values ('xyz-e01',3)")
    df = sql(mvQuery)
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "dm1"))
    checkAnswer(df, Seq(Row("xyz-e01", 10, 2.5, 4)))

    mvQuery = "select tags_id , avg(value),count(value),max(value) from source group by tags_id"
    sql("drop materialized view if exists dm1")
    sql(s"create materialized view dm1  as $mvQuery")
    result = sql("show materialized views on table source").collectAsList()
    assert(result.get(0).get(3).toString.equalsIgnoreCase("incremental"))
    sql("insert into source values ('xyz-e01',3)")
    df = sql(mvQuery)
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "dm1"))
    checkAnswer(df, Seq(Row("xyz-e01", 2.6, 5, 3)))
    sql("drop materialized view if exists dm1")
    sql("drop table if exists source")
  }

  test("test create MV with average and query with sum column") {
    sql("drop table if exists source")
    sql(
      "create table if not exists source (tags_id STRING, value DOUBLE) stored as carbondata")
    sql("insert into source values ('xyz-e01',3)")
    sql("insert into source values ('xyz-e01',1)")
    val mvQuery = "select tags_id, avg(value) from source group by tags_id"
    sql("drop materialized view if exists dm1")
    sql(s"create materialized view dm1  as $mvQuery")
    sql("insert into source values ('xyz-e01',3)")
    val df = sql("select tags_id, sum(value) from source group by tags_id")
    assert(isTableAppearedInPlan(df.queryExecution.optimizedPlan, "dm1"))
    checkAnswer(df, Seq(Row("xyz-e01", 7)))
    sql("drop materialized view if exists dm1")
    sql("drop table if exists source")
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

  test("test drop mv must fail if not exists") {
    val ex = intercept[MalformedMVCommandException] {
      sql("drop materialized view MV_notPresent")
    }
    assert(ex.getMessage.contains("Materialized view with name default.MV_notPresent does not exists"))
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
    sql("set carbon.enable.mv = false")
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
  // scalastyle:on lineLength
}

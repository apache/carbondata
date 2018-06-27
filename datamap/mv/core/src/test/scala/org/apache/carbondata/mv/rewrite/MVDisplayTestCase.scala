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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties


class MVDisplayTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
  }

  test("Display error: insert and SELECT data after rebuild datamap") {
    sql("DROP DATAMAP IF EXISTS mv1")
    sql("DROP TABLE IF EXISTS origin_table")

    sql("CREATE TABLE origin_table ( name STRING, age INT) STORED BY 'carbondata'")
    sql("INSERT INTO origin_table SELECT 'babu',12")
    sql("CREATE datamap mv1 USING 'mv' AS SELECT name FROM origin_table ")
    sql("REBUILD DATAMAP mv1")

    assert(sql("EXPLAIN SELECT name FROM origin_table").rdd
      .collect()(1).toString().contains("Table name :mv1_table"))
    checkAnswer(sql("SELECT name FROM origin_table"),
      Seq(Row("babu")))

    sql("INSERT INTO origin_table SELECT 'lal',13")
    checkAnswer(sql("SELECT name FROM origin_table"),
      Seq(Row("babu"), Row("lal")))
    assert(sql("EXPLAIN SELECT name FROM origin_table").rdd
      .collect()(1).toString().contains("Table name :origin_table"))

    sql("INSERT INTO origin_table SELECT 'babu',12")
    checkAnswer(sql("SELECT name FROM origin_table"),
      Seq(Row("babu"), Row("lal"), Row("babu")))
    assert(sql("EXPLAIN SELECT name FROM origin_table").rdd
      .collect()(1).toString().contains("Table name :origin_table"))

    sql("REBUILD DATAMAP mv1")
    val df = sql("SELECT name FROM origin_table")
    val analyzed = df.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "mv1"))
    checkAnswer(sql("SELECT name FROM origin_table"),
      Seq(Row("babu"), Row("lal"), Row("babu")))

    sql("INSERT INTO origin_table SELECT 'babu',12")
    checkAnswer(sql("SELECT name FROM origin_table"),
      Seq(Row("babu"), Row("lal"), Row("babu"), Row("babu")))
    assert(sql("EXPLAIN SELECT name FROM origin_table").rdd
      .collect()(1).toString().contains("Table name :origin_table"))

    sql("DROP DATAMAP IF EXISTS mv1")
    sql("DROP TABLE IF EXISTS origin_table")
  }

  test("Display error: load and SELECT data after rebuild datamap") {
    val projectPath = new File(this.getClass.getResource("/").getPath + "../../../../../")
      .getCanonicalPath.replaceAll("\\\\", "/")
    val integrationPath = s"$projectPath/integration"
    val resourcesPath = s"$integrationPath/spark-common-test/src/test/resources"
    sql("DROP DATAMAP IF EXISTS mv1")
    sql("DROP TABLE IF EXISTS fact_table1")
    sql(
      """
        | CREATE TABLE fact_table1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      s"""
         | LOAD DATA local inpath '$resourcesPath/data_big.csv'
         | INTO TABLE fact_table1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')
       """.stripMargin)

    sql("CREATE DATAMAP mv1 USING 'mv' AS SELECT empname, designation FROM fact_table1")
    sql(s"REBUILD DATAMAP mv1")
    val df = sql("SELECT empname,designation FROM fact_table1")
    assert(90 == df.collect().length)
    assert(verifyMVDataMap(df.queryExecution.analyzed, "mv1"))

    sql(
      s"""
         | LOAD DATA local inpath '$resourcesPath/data_big.csv'
         | INTO TABLE fact_table1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')
       """.stripMargin)

    val df2 = sql("SELECT empname,designation FROM fact_table1")
    assert(180 == df2.collect().length)
    assert(!verifyMVDataMap(df2.queryExecution.analyzed, "mv1"))
    sql("DROP DATAMAP IF EXISTS mv1")
    sql("DROP TABLE IF EXISTS fact_table1")
  }

  def verifyMVDataMap(logicalPlan: LogicalPlan, dataMapName: String): Boolean = {
    val tables = logicalPlan collect {
      case l: LogicalRelation => l.catalogTable.get
    }
    tables.exists(_.identifier.table.equalsIgnoreCase(dataMapName + "_table"))
  }

  def drop(): Unit = {
    sql("DROP DATAMAP IF EXISTS mv1")
    sql("DROP TABLE IF EXISTS fact_table1")
    sql("DROP TABLE IF EXISTS origin_table")
  }

  override def afterAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }
}

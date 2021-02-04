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

package org.apache.carbondata.spark.testsuite.createTable

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * test functionality for alter table with indexSchema
 */
class TestRenameTableWithIndex extends QueryTest with BeforeAndAfterAll {

  val smallFile = s"$resourcesPath/sample.csv"

  override def beforeAll {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS carbon_tb")
    sql("DROP TABLE IF EXISTS fact_table1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
  }

  test("Creating a bloomfilter, SI indexSchema,then table rename") {
    sql(
      s"""
         | CREATE TABLE carbon_table(
         | id INT, name String, city String, age INT
         | )
         | STORED AS carbondata
       """.stripMargin)

    sql(
      s"""
         | CREATE INDEX dm_carbon_table_name
         | ON TABLE carbon_table (name, city)
         | AS 'bloomfilter'
         | Properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    (1 to 2).foreach { i =>

      sql(
        s"""
           | insert into carbon_table select 5,'bb','beijing',21
           | """.stripMargin)

      sql(
        s"""
           | insert into carbon_table select 6,'cc','shanghai','29'
           | """.stripMargin)
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE carbon_table
           | OPTIONS('header'='false')
         """.stripMargin)
    }

    sql(
      s"""
         | show indexes on table carbon_table
       """.stripMargin).collect()

    sql(
      s"""
         | select * from carbon_table where name='eason'
       """.stripMargin).collect()

    sql(
      s"""
         | explain select * from carbon_table where name='eason'
       """.stripMargin).collect()

    sql("CREATE INDEX dm_carbon_si ON TABLE carbon_table (name, city) AS 'carbondata'")

    sql(
      s"""
         | alter TABLE carbon_table rename to carbon_tb
       """.stripMargin)

    sql(
      s"""
         | show indexes on table carbon_tb
       """.stripMargin).collect()

    sql(
      s"""
         | select * from carbon_tb where name='eason'
       """.stripMargin).collect()

    sql(
      s"""
         | explain select * from carbon_tb where name='eason'
       """.stripMargin).collect()

    checkExistence(sql(s"""show indexes on table carbon_tb""".stripMargin),
      true, "dm_carbon_si")
  }

  /*
   * mv indexSchema does not support running here, now must run in mv project.
  test("Creating a mv indexSchema,then table rename") {
    sql(
      """
        | CREATE TABLE fact_table2 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table2
        OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE fact_table2
        OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql("drop materialized view if exists mv1")
    sql("create materialized view mv1 as select empname, designation from fact_table2")
    sql(s"refresh materialized view mv1")

    sql(
      s"""
         | show materialized views on table fact_table2
       """.stripMargin).collect()

    val exception_tb_rename: Exception = intercept[Exception] {
      sql(
        s"""
           | alter TABLE fact_table2 rename to fact_tb2
       """.stripMargin)
    }
    assert(exception_tb_rename.getMessage
      .contains("alter rename is not supported for mv indexSchema"))
  } */

  override def afterAll: Unit = {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS carbon_tb")
    sql("DROP TABLE IF EXISTS fact_table1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
        CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT)
  }
}

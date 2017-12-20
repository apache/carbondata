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

package org.apache.carbondata.integration.spark.testsuite.preaggregate

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestPreAggregateExpressions extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS mainTable")
    sql("CREATE TABLE mainTable(id int, name string, city string, age string) STORED BY 'org.apache.carbondata.format'")
  }
  test("test pre agg create table with expression 1") {
    sql(
      s"""
         | CREATE DATAMAP agg0 ON TABLE mainTable USING 'preaggregate' AS
         | SELECT name,
         | count(age)
         | FROM mainTable GROUP BY name
         | """.stripMargin)
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg0"), true, "maintable_age_count")
  }

  test("test pre agg create table with expression 2") {
    sql(
      s"""
         | CREATE DATAMAP agg1 ON TABLE mainTable USING 'preaggregate' AS
         | SELECT name,
         | sum(CASE WHEN age=35 THEN id ELSE 0 END)
         | FROM mainTable GROUP BY name
         | """.stripMargin)
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg1"), true, "maintable_column_0_sum")
  }

  test("test pre agg create table with expression 3") {
    sql(
      s"""
         | CREATE DATAMAP agg2 ON TABLE mainTable USING 'preaggregate' AS
         | SELECT name,
         | sum(CASE WHEN age=35 THEN id ELSE 0 END),
         | city
         | FROM mainTable GROUP BY name,city
         | """.stripMargin)
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg2"), true, "maintable_column_0_sum")
  }

  test("test pre agg create table with expression 4") {
    sql(
      s"""
         | CREATE DATAMAP agg3 ON TABLE mainTable USING 'preaggregate' AS
         | SELECT name,
         | sum(CASE WHEN age=27 THEN id ELSE 0 END)
         | FROM mainTable GROUP BY name
         | """.stripMargin)
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg3"), true, "maintable_column_0_sum")
  }

  test("test pre agg create table with expression 5") {
    sql(
      s"""
         | CREATE DATAMAP agg4 ON TABLE mainTable USING 'preaggregate' AS
         | SELECT name,
         | sum(CASE WHEN age=27 THEN id ELSE 0 END),
         | SUM(CASE WHEN age=35 THEN id ELSE 0 END)
         | FROM mainTable GROUP BY name
         | """.stripMargin)
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg4"), true, "maintable_column_0_sum")
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg4"), true, "maintable_column_1_sum")
  }

  test("test pre agg create table with expression 6") {
    sql(
      s"""
         | CREATE DATAMAP agg5 ON TABLE mainTable USING 'preaggregate' AS
         | SELECT name,
         | COUNT(CASE WHEN age=27 THEN(CASE WHEN name='eason' THEN id ELSE 0 END) ELSE 0 END)
         | FROM mainTable GROUP BY name
         | """.stripMargin)
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg5"), true, "maintable_column_0_count")
  }

  override def afterAll: Unit = {
    sql("DROP TABLE IF EXISTS mainTable")
  }

}

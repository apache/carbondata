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
package org.apache.carbondata.spark.testsuite.ShowTable

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException

/**
 * Test class for show tables.
 */
class TestShowTable extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {
  val dbName = "testshowtable"
  override def beforeAll: Unit = {
    sql("use default")
    sql(s"drop database if exists $dbName cascade")
    sql(s"create database $dbName")
    sql(s"use $dbName")
  }

  test("test show tables") {
    sql("create table employee(id string, name string) stored as carbondata")
    sql("create table employee_part(name string) partitioned by (grade int)")
    sql("create index employee_si on table employee(name) as 'carbondata'")
    sql("create materialized view employee_mv as select name from employee group by name")
    val rows = sql("show tables").collect()
    val schema = rows(0).schema
    assert(schema.length == 3)
    assert(schema(0).name.equals("database"))
    assert(schema(1).name.equals("tableName"))
    assert(schema(2).name.equals("isTemporary"))
    // show tables query can return views as well. Just validate if expected rows are present and
    // mv row is not present
    val expectedRows = Seq(Row(dbName, "employee", false),
      Row(dbName, "employee_si", false),
      Row(dbName, "employee_part", false))
    assert(rows.intersect(expectedRows).length == 3)
    assert(rows.intersect(Seq(Row(dbName, "employee_mv", false))).length == 0)
  }

  test("test show table extended like") {
    sql("create table employee(id string, name string) stored as carbondata")
    sql("create table employee_part(name string) partitioned by (grade int)")
    sql("create index employee_si on table employee(name) as 'carbondata'")
    sql("create materialized view employee_mv as select name from employee group by name")
    val rows = sql("show table extended like 'emp*'").collect()
    assert(rows.length == 3)
    val schema = rows(0).schema
    assert(schema.length == 4)
    assert(schema(0).name.equals("database"))
    assert(schema(1).name.equals("tableName"))
    assert(schema(2).name.equals("isTemporary"))
    assert(schema(3).name.equals("information"))
  }

  override def afterEach(): Unit = {
    drop
  }
  override def afterAll(): Unit = {
    sql("use default")
    sql(s"drop database if exists $dbName cascade")
  }

  def drop(): Unit = {
    scala.util.control.Exception.ignoring(classOf[MalformedIndexCommandException]) {
      sql("drop index employee_si on employee")
    }
    sql("drop materialized view if exists employee_mv1")
    sql("drop table if exists employee")
    sql("drop table if exists employee_part")
  }
}

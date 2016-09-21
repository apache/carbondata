/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.testsuite.deleteTable

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestDeleteAllCarbonTablesInDB extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("create database if not exists testDB")
    sql("use testDB")
    sql("drop table if exists carbonTB1")
    sql("drop table if exists carbonTB2")
    sql("drop table if exists hiveTB")
    sql("CREATE TABLE if not exists carbonTB1 (name string, age int) STORED BY 'org.apache.carbondata.format'")
    sql("CREATE TABLE if not exists carbonTB2 (name string, age int) STORED BY 'org.apache.carbondata.format'")
    sql("CREATE TABLE if not exists hiveTB (name string, age int)")
  }

  test("Drop all carbon tables in one database") {
    sql("DROP ALL CARBON TABLES IN testDB")
    checkAnswer(
      sql("show tables in testDB"), Seq(Row("hivetb", false))
    )
  }

  override def afterAll {
    sql("drop table if exists carbonTB1")
    sql("drop table if exists carbonTB2")
    sql("drop table if exists hiveTB")
    sql("drop database testDB")
    sql("use default")
  }
}

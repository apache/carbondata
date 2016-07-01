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

package org.carbondata.spark.testsuite.bigdecimal

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties

/**
  * Test cases for testing big decimal functionality
  */
class TestBigDecimal extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql("CREATE TABLE IF NOT EXISTS carbonTable (ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary Decimal(17,2))STORED BY 'org.apache.carbondata.format'")
    sql("create table if not exists hiveTable(ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary Decimal(17,2))row format delimited fields terminated by ','")
    sql("LOAD DATA LOCAL INPATH './src/test/resources/decimalDataWithHeader.csv' into table carbonTable")
    sql("LOAD DATA local inpath './src/test/resources/decimalDataWithoutHeader.csv' INTO table hiveTable")
  }

  test("test detail query on big decimal column") {
    checkAnswer(sql("select salary from carbonTable order by salary"),
      sql("select salary from hiveTable order by salary"))
  }

  test("test sum function on big decimal column") {
    checkAnswer(sql("select sum(salary) from carbonTable"),
      sql("select sum(salary) from hiveTable"))
  }

  test("test max function on big decimal column") {
    checkAnswer(sql("select max(salary) from carbonTable"),
      sql("select max(salary) from hiveTable"))
  }

  test("test min function on big decimal column") {
    checkAnswer(sql("select min(salary) from carbonTable"),
      sql("select min(salary) from hiveTable"))
  }
  
  test("test min datatype on big decimal column") {
    val output = sql("select min(salary) from carbonTable").collectAsList().get(0).get(0)
    assert(output.isInstanceOf[java.math.BigDecimal])
  }

  test("test max datatype on big decimal column") {
    val output = sql("select max(salary) from carbonTable").collectAsList().get(0).get(0)
    assert(output.isInstanceOf[java.math.BigDecimal])
  }
  
  test("test count function on big decimal column") {
    checkAnswer(sql("select count(salary) from carbonTable"),
      sql("select count(salary) from hiveTable"))
  }

  test("test distinct function on big decimal column") {
    checkAnswer(sql("select distinct salary from carbonTable order by salary"),
      sql("select distinct salary from hiveTable order by salary"))
  }

  test("test sum-distinct function on big decimal column") {
    checkAnswer(sql("select sum(distinct salary) from carbonTable"),
      sql("select sum(distinct salary) from hiveTable"))
  }

  test("test count-distinct function on big decimal column") {
    checkAnswer(sql("select count(distinct salary) from carbonTable"),
      sql("select count(distinct salary) from hiveTable"))
  }

  override def afterAll {
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}



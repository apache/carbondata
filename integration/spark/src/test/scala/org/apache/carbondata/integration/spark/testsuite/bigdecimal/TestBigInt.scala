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

package org.apache.carbondata.spark.testsuite.bigdecimal

import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
  * Test cases for testing big int functionality
  */
class TestBigInt extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql("CREATE TABLE IF NOT EXISTS carbonTable (ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary bigint)STORED AS carbondata")
    sql("create table if not exists hiveTable(ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary bigint)row format delimited fields terminated by ','")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/bigIntDataWithHeader.csv' into table carbonTable")
    sql(s"LOAD DATA local inpath '$resourcesPath/bigIntDataWithoutHeader.csv' INTO table hiveTable")
  }

  test("test detail query on big int column") {
    checkAnswer(sql("select salary from carbonTable order by salary"),
      sql("select salary from hiveTable order by salary"))
  }

  test("test sum function on big int column") {
    checkAnswer(sql("select sum(salary) from carbonTable"),
      sql("select sum(salary) from hiveTable"))
  }

  test("test max function on big int column") {
    checkAnswer(sql("select max(salary) from carbonTable"),
      sql("select max(salary) from hiveTable"))
  }

  test("test min function on big int column") {
    checkAnswer(sql("select min(salary) from carbonTable"),
      sql("select min(salary) from hiveTable"))
  }

  test("test count function on big int column") {
    checkAnswer(sql("select count(salary) from carbonTable"),
      sql("select count(salary) from hiveTable"))
  }

  test("test distinct function on big int column") {
    checkAnswer(sql("select distinct salary from carbonTable order by salary"),
      sql("select distinct salary from hiveTable order by salary"))
  }

  test("test sum-distinct function on big int column") {
    checkAnswer(sql("select sum(distinct salary) from carbonTable"),
      sql("select sum(distinct salary) from hiveTable"))
  }

  test("test count-distinct function on big int column") {
    checkAnswer(sql("select count(distinct salary) from carbonTable"),
      sql("select count(distinct salary) from hiveTable"))
  }

  test("test big int data type storage for boundary values") {
    sql("DROP TABLE IF EXISTS Test_Boundary_carbon")
    sql("DROP TABLE IF EXISTS Test_Boundary_hive")
    sql("create table Test_Boundary_carbon (c1_String string, c2_Bigint Bigint) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/testBigInt_boundary_value.csv' into table Test_Boundary_carbon OPTIONS('FILEHEADER'='c1_String,c2_Bigint')")
    sql("create table Test_Boundary_hive (c1_String string, c2_Bigint Bigint) row format delimited fields terminated by ','")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/testBigInt_boundary_value.csv' into table Test_Boundary_hive")
    checkAnswer(sql("select c2_bigInt*23 from Test_Boundary_carbon where c2_BigInt<9223372036854775807"),
      sql("select c2_bigInt*23 from Test_Boundary_hive where c2_BigInt<9223372036854775807"))
    sql("DROP TABLE IF EXISTS Test_Boundary_carbon")
    sql("DROP TABLE IF EXISTS Test_Boundary_hive")
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
  }
}



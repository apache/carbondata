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

import java.math.BigDecimal

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
  * Test cases for testing big decimal functionality
  */
class TestBigDecimal extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
    sql("drop table if exists hiveBigDecimal")
    sql("drop table if exists carbonBigDecimal_2")
    sql("DROP TABLE IF EXISTS decimal_int_test")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.SORT_SIZE, "1")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT, "2")
    sql("CREATE TABLE IF NOT EXISTS carbonTable (ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary Decimal(17,2))STORED AS carbondata")
    sql("create table if not exists hiveTable(ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary Decimal(17,2))row format delimited fields terminated by ','")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/decimalDataWithHeader.csv' into table carbonTable")
    sql(s"LOAD DATA local inpath '$resourcesPath/decimalDataWithoutHeader.csv' INTO table hiveTable")
    sql("create table if not exists hiveBigDecimal(ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary decimal(30, 10))row format delimited fields terminated by ','")
    sql(s"LOAD DATA local inpath '$resourcesPath/decimalBoundaryDataHive.csv' INTO table hiveBigDecimal")
    sql("create table if not exists carbonBigDecimal_2 (ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary decimal(30, 10)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/decimalBoundaryDataCarbon.csv' into table carbonBigDecimal_2")

    sql("create table if not exists carbonBigDecimal_3 (ID Int, date Timestamp, country String,name String, phonetype String, serialname String, salary decimal(30, 2)) STORED AS carbondata ")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/decimalBoundaryDataCarbon.csv' into table carbonBigDecimal_3")
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
  test("test filter query on big decimal column") {
    // equal to
    checkAnswer(sql("select salary from carbonTable where salary=45234525465882.24"),
      sql("select salary from hiveTable where salary=45234525465882.24"))
    // greater than
    checkAnswer(sql("select salary from carbonTable where salary>15000"),
      sql("select salary from hiveTable where salary>15000"))
    // greater than equal to
    checkAnswer(sql("select salary from carbonTable where salary>=15000.43525"),
      sql("select salary from hiveTable where salary>=15000.43525"))
    // less than
    checkAnswer(sql("select salary from carbonTable where salary<45234525465882"),
      sql("select salary from hiveTable where salary<45234525465882"))
    // less than equal to
    checkAnswer(sql("select salary from carbonTable where salary<=45234525465882.24"),
      sql("select salary from hiveTable where salary<=45234525465882.24"))
  }

  test("test aggregation on big decimal column with increased precision") {
    sql("drop table if exists carbonBigDecimal")
    sql("create table if not exists carbonBigDecimal (ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary decimal(27, 10)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/decimalBoundaryDataCarbon.csv' into table carbonBigDecimal")

    checkAnswer(sql("select sum(salary) from carbonBigDecimal"),
      sql("select sum(salary) from hiveBigDecimal"))

    checkAnswer(sql("select sum(distinct salary) from carbonBigDecimal"),
      sql("select sum(distinct salary) from hiveBigDecimal"))

    sql("drop table if exists carbonBigDecimal")
  }

  test("test big decimal for dictionary look up") {
    sql("drop table if exists decimalDictLookUp")
    sql("create table if not exists decimalDictLookUp (ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary decimal(27, 10)) STORED AS carbondata ")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/decimalBoundaryDataCarbon.csv' into table decimalDictLookUp")

    checkAnswer(sql("select sum(salary) from decimalDictLookUp"),
      sql("select sum(salary) from hiveBigDecimal"))

    sql("drop table if exists decimalDictLookUp")
  }

  test("test sum+10 aggregation on big decimal column with high precision") {
    checkAnswer(sql("select sum(salary)+10 from carbonBigDecimal_2"),
      sql("select sum(salary)+10 from hiveBigDecimal"))
  }

  test("test sum*10 aggregation on big decimal column with high precision") {
    checkAnswer(sql("select cast(sum(salary)*10 as double) from carbonBigDecimal_2"),
      sql("select cast(sum(salary)*10 as double) from hiveBigDecimal"))
  }

  test("test sum/10 aggregation on big decimal column with high precision") {
    checkAnswer(sql("select sum(salary)/10 from carbonBigDecimal_2"),
      sql("select sum(salary)/10 from hiveBigDecimal"))
  }

  test("test sum-distinct+10 aggregation on big decimal column with high precision") {
    checkAnswer(sql("select sum(distinct(salary))+10 from carbonBigDecimal_2"),
      sql("select sum(distinct(salary))+10 from hiveBigDecimal"))
  }

  test("test sum-distinct*10 aggregation on big decimal column with high precision") {
    checkAnswer(sql("select cast(sum(distinct(salary))*10 as decimal)from carbonBigDecimal_2"),
      sql("select cast(sum(distinct(salary))*10 as decimal) from hiveBigDecimal"))
  }

  test("test sum-distinct/10 aggregation on big decimal column with high precision") {
    checkAnswer(sql("select sum(distinct(salary))/10 from carbonBigDecimal_2"),
      sql("select sum(distinct(salary))/10 from hiveBigDecimal"))
  }

  test("test avg+10 aggregation on big decimal column with high precision") {
    checkAnswer(sql("select avg(salary)+10 from carbonBigDecimal_2"),
      sql("select avg(salary)+10 from hiveBigDecimal"))
  }

  test("test avg*10 aggregation on big decimal column with high precision") {
    checkAnswer(sql("select avg(salary)*10 from carbonBigDecimal_2"),
      sql("select avg(salary)*10 from hiveBigDecimal"))
  }

  test("test avg/10 aggregation on big decimal column with high precision") {
    checkAnswer(sql("select avg(salary)/10 from carbonBigDecimal_2"),
      sql("select avg(salary)/10 from hiveBigDecimal"))
  }

  test("test lower precision definiton on big decimal column with high precision value") {
    checkAnswer(sql("select count(salary) from carbonBigDecimal_3"),
      sql("select count(salary) from hiveBigDecimal"))
  }

  test("test decimal compression where both precision and data falls in integer range") {
    sql("create table decimal_int_test(d1 decimal(9,3)) STORED AS carbondata")
    sql(s"load data inpath '$resourcesPath/decimal_int_range.csv' into table decimal_int_test")
    sql("select * from decimal_int_test").show(false)
    checkAnswer(sql("select * from decimal_int_test"),
      Seq(Row(new BigDecimal("111111.000")),
        Row(new BigDecimal("222222.120")),
        Row(new BigDecimal("333333.123"))))
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.SORT_SIZE,
        CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL)
      .addProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT,
        CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE)
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
    sql("drop table if exists hiveBigDecimal")
    sql("drop table if exists carbonBigDecimal_2")
    sql("DROP TABLE IF EXISTS decimal_int_test")
    sql("drop table if exists carbonBigDecimal_3")
  }
}



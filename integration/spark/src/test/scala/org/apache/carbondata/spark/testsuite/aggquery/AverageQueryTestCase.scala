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
package org.apache.carbondata.spark.testsuite.aggquery

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
 * test cases for aggregate query
 */
class AverageQueryTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
    sql("""
       CREATE TABLE carbonTable (ID int, date timeStamp, country string, count int,
       phonetype string, serialname string, salary double)
       STORED AS carbondata""")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/avgTest.csv' INTO table carbonTable""")

    // create a hive table for compatible check
    sql("""
       CREATE TABLE hiveTable (ID int, date timeStamp, country string, count int,
       phonetype string, serialname string, salary double)
       row format delimited fields terminated by ','""")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/avgTest.csv' INTO table hiveTable")
  }

  test("select avg(Measure_IntType)+IntType from carbonTable") {
    checkAnswer(
      sql("SELECT avg(count)+10 FROM carbonTable"),
      sql("SELECT avg(count)+10 FROM hiveTable"))
  }

  test("select avg(Dimension_IntType)+IntType from table") {
    checkAnswer(
      sql("SELECT avg(ID)+10 FROM carbonTable"),
      sql("SELECT avg(ID)+10 FROM hiveTable"))
  }

  test("select avg(TimeStamp)+IntType from table") {
    checkAnswer(
      sql("SELECT avg(date)+10 FROM carbonTable"),
      sql("SELECT avg(date)+10 FROM hiveTable"))
  }

  test("select avg(TimeStamp) from table") {
    checkAnswer(
      sql("SELECT avg(date) FROM carbonTable"),
      sql("SELECT avg(date) FROM hiveTable"))
  }

  test("select avg(StringType)+IntType from table") {
    checkAnswer(
      sql("SELECT avg(country)+10 FROM carbonTable"),
      sql("SELECT avg(country)+10 FROM hiveTable"))
  }

  test("select max(StringType)+IntType from table") {
    checkAnswer(
      sql("SELECT max(country)+10 FROM carbonTable"),
      sql("SELECT max(country)+10 FROM hiveTable"))
  }

  test("select min(StringType)+IntType from table") {
    checkAnswer(
      sql("SELECT min(country)+10 FROM carbonTable"),
      sql("SELECT min(country)+10 FROM hiveTable"))
  }

  test("select sum(StringType)+IntType from table") {
    checkAnswer(
      sql("SELECT sum(country)+10 FROM carbonTable"),
      sql("SELECT sum(country)+10 FROM hiveTable"))
  }

  test("select sum(distinct StringType)+IntType from table") {
    checkAnswer(
      sql("SELECT sum(distinct country)+10 FROM carbonTable"),
      sql("SELECT sum(distinct country)+10 FROM hiveTable"))
  }

  test("group by with having") {
    checkAnswer(
      sql("select country,count(*) from carbonTable group by country having count(*)>5"),
      Seq(Row("china", 9)))
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table carbonTable")
    sql("drop table hiveTable")
  }

}

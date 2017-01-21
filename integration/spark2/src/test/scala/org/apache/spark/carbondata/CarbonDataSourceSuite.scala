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

package org.apache.spark.carbondata

import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class CarbonDataSourceSuite extends QueryTest with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    // Drop table
    sql("DROP TABLE IF EXISTS carbon_testtable")
    sql("DROP TABLE IF EXISTS csv_table")

    // Create table
    sql(
      s"""
         | CREATE TABLE carbon_testtable(
         |    shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string,
         |    decimalField decimal(13, 0),
         |    timestampField string)
         | USING org.apache.spark.sql.CarbonSource
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE csv_table
         | (  shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string,
         |    decimalField decimal(13, 0),
         |    timestampField string)
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       """.stripMargin)
  }

  override def afterAll(): Unit = {
    sql("drop table carbon_testtable")
    sql("DROP TABLE IF EXISTS csv_table")
  }

  test("project") {
    sql("select * from carbon_testtable").collect()
  }

  test("agg") {
    sql("select stringField, sum(intField) , sum(decimalField) " +
      "from carbon_testtable group by stringField").collect()

    sql(
      s"""
         | INSERT INTO TABLE carbon_testtable
         | SELECT shortField, intField, bigintField, doubleField, stringField,
         | decimalField, from_unixtime(unix_timestamp(timestampField,'yyyy/M/dd')) timestampField
         | FROM csv_table
       """.stripMargin)
  }

}

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

package org.apache.carbondata.spark.testsuite.detailquery

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for detailed query on Int and BigInt of No Dictionary col
  *
  */
class NoDictionaryColumnTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS carbonTable")
    sql("DROP TABLE IF EXISTS hiveTable")
    sql("DROP TABLE IF EXISTS carbonEmpty")
    sql("DROP TABLE IF EXISTS hiveEmpty")
    sql("CREATE TABLE carbonTable (imei String, age Int, num BigInt) STORED AS carbondata ")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/datawithNegtiveNumber.csv' INTO TABLE carbonTable")
    sql("CREATE TABLE hiveTable (imei String, age Int, num BigInt) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/datawithNegeativewithoutHeader.csv' INTO TABLE hiveTable")

    sql("CREATE TABLE carbonEmpty (cust_id int, cust_name String, active_emui_version String, bob timestamp, bigint_column bigint) STORED AS carbondata ")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/dataWithEmptyRows.csv' INTO TABLE carbonEmpty OPTIONS('FILEHEADER'='cust_id,cust_name,active_emui_version,bob,bigint_column')")
    sql("CREATE TABLE hiveEmpty (cust_id int, cust_name String, active_emui_version String, bob timestamp, bigint_column bigint) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/dataWithEmptyRows.csv' INTO TABLE hiveEmpty")
  }

  test("SELECT IntType FROM carbonTable") {
    checkAnswer(
      sql("SELECT imei,age FROM carbonTable ORDER BY imei"),
      sql("SELECT imei,age FROM hiveTable ORDER BY imei")
    )
  }

  test("SELECT BigIntType FROM carbonTable") {
    checkAnswer(
      sql("SELECT imei,num FROM carbonTable ORDER BY imei"),
      sql("SELECT imei,num FROM hiveTable ORDER BY imei")
    )
  }

  test("test load data with one row that all no dictionary column values are empty") {
    checkAnswer(
      sql("SELECT cust_name,active_emui_version FROM carbonEmpty"),
      sql("SELECT cust_name,active_emui_version FROM hiveEmpty")
    )
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS carbonTable")
    sql("DROP TABLE IF EXISTS hiveTable")
    sql("DROP TABLE IF EXISTS carbonEmpty")
    sql("DROP TABLE IF EXISTS hiveEmpty")
  }
}
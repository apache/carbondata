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

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestDataLoadWithOverWrite extends QueryTest with BeforeAndAfterAll {
  val testData = s"$resourcesPath/sample.csv"

  override def beforeAll() {
    sql("DROP TABLE IF EXISTS carbon_load_overwrite")
  }

  test("01 load with overwrite command, check overwrite covering load segments") {
    sql("DROP TABLE IF EXISTS carbon_load_overwrite")
    sql(
      """
        | CREATE TABLE carbon_load_overwrite(id int, name string, city string, age int)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$testData' into table carbon_load_overwrite")
    val result1 = sql(s"select * from carbon_load_overwrite").collect()
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table carbon_load_overwrite")
    sql(s"LOAD DATA LOCAL INPATH '$testData' overwrite into table carbon_load_overwrite")
    val result2 = sql(s"select * from carbon_load_overwrite").collect()
    assert(result1 sameElements result2)
  }

  test("02 load with overwrite command, check overwrite covering overwrite segments") {
    sql("DROP TABLE IF EXISTS carbon_load_overwrite")
    sql(
      """
        | CREATE TABLE carbon_load_overwrite(id int, name string, city string, age int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$testData' overwrite into table carbon_load_overwrite")
    val result1 = sql(s"select * from carbon_load_overwrite").collect()
    sql(s"LOAD DATA LOCAL INPATH '$testData' overwrite into table carbon_load_overwrite")
    sql(s"LOAD DATA LOCAL INPATH '$testData' overwrite into table carbon_load_overwrite")
    val result2 = sql(s"select * from carbon_load_overwrite").collect()
    assert(result1 sameElements result2)
  }

  override protected def afterAll() {
    sql("DROP TABLE IF EXISTS carbon_load_overwrite")
  }
}

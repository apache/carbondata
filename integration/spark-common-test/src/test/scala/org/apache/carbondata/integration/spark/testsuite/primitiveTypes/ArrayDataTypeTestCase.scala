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
package org.apache.carbondata.integration.spark.testsuite.primitiveTypes

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for filter query on Float datatypes
 */
class ArrayDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS datatype_array_parquet")
    sql("DROP TABLE IF EXISTS datatype_array_carbondata")
  }

  test("test when insert select from a parquet table with an array with empty string") {
    sql("create table datatype_array_parquet(col array<string>) stored as parquet")
    sql("create table datatype_array_carbondata(col array<string>) stored as carbondata")
    sql("insert into datatype_array_parquet values(array(''))")
    sql("insert into datatype_array_carbondata select * from datatype_array_parquet")
    checkAnswer(
      sql("SELECT * FROM datatype_array_carbondata"),
      sql("SELECT * FROM datatype_array_parquet"))
    sql("DROP TABLE IF EXISTS datatype_array_carbondata")
    sql("DROP TABLE IF EXISTS datatype_array_parquet")
  }

  test("test when insert select from a parquet table with empty array") {
    sql("create table datatype_array_parquet(col array<string>) stored as parquet")
    sql("create table datatype_array_carbondata(col array<string>) stored as carbondata")
    sql("insert into datatype_array_parquet values(array())")
    sql("insert into datatype_array_carbondata select * from datatype_array_parquet")
    checkAnswer(
      sql("SELECT * FROM datatype_array_carbondata"),
      sql("SELECT * FROM datatype_array_parquet"))
    sql("DROP TABLE IF EXISTS datatype_array_carbondata")
    sql("DROP TABLE IF EXISTS datatype_array_parquet")
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS datatype_array_carbondata")
    sql("DROP TABLE IF EXISTS datatype_array_parquet")
  }
}
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

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for filter query on Float datatypes
 */
class MapDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS datatype_map_parquet")
    sql("DROP TABLE IF EXISTS datatype_map_carbondata")
  }

  test("test when insert select from a parquet table with an map with empty key and value") {
    sql("create table datatype_map_parquet(col map<string,string>) stored as parquet")
    sql("create table datatype_map_carbondata(col map<string,string>) stored as carbondata")
    sql("insert into datatype_map_parquet values(map('',''))")
    sql("insert into datatype_map_carbondata select * from datatype_map_parquet")
    checkAnswer(
      sql("SELECT * FROM datatype_map_carbondata"),
      sql("SELECT * FROM datatype_map_parquet"))
    sql("DROP TABLE IF EXISTS datatype_map_carbondata")
    sql("DROP TABLE IF EXISTS datatype_map_parquet")
  }

  test("test when insert select from a parquet table with an map with empty key") {
    sql("create table datatype_map_parquet(col map<string,string>) stored as parquet")
    sql("create table datatype_map_carbondata(col map<string,string>) stored as carbondata")
    sql("insert into datatype_map_parquet values(map('','value'))")
    sql("insert into datatype_map_carbondata select * from datatype_map_parquet")
    checkAnswer(
      sql("SELECT * FROM datatype_map_carbondata"),
      sql("SELECT * FROM datatype_map_parquet"))
    sql("DROP TABLE IF EXISTS datatype_map_carbondata")
    sql("DROP TABLE IF EXISTS datatype_map_parquet")
  }

  test("test when insert select from a parquet table with an map with empty value") {
    sql("create table datatype_map_parquet(col map<string,string>) stored as parquet")
    sql("create table datatype_map_carbondata(col map<string,string>) stored as carbondata")
    sql("insert into datatype_map_parquet values(map('key',''))")
    sql("insert into datatype_map_carbondata select * from datatype_map_parquet")
    checkAnswer(
      sql("SELECT * FROM datatype_map_carbondata"),
      sql("SELECT * FROM datatype_map_parquet"))
    sql("DROP TABLE IF EXISTS datatype_map_carbondata")
    sql("DROP TABLE IF EXISTS datatype_map_parquet")
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS datatype_map_carbondata")
    sql("DROP TABLE IF EXISTS datatype_map_parquet")
  }
}
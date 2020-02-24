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

package org.apache.carbondata.spark.testsuite.createTable

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test functionality of create table with column comment
 */
class TestCreateTableWithColumnComment extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("use default")
    sql("drop table if exists columnComment")
    sql("drop table if exists defaultComment")
  }

  test("test create table with column comment") {
    sql(
      "create table columnComment(id int, name string comment \"This column is called name\") " +
      "STORED AS carbondata")
    checkExistence(sql("describe formatted columnComment"), true, "This column is called name")
  }

  test("test create table with default column comment value") {
    sql(
      "create table defaultComment(id int, name string) " +
      "STORED AS carbondata")
    checkExistence(sql("describe formatted defaultComment"), true, "null")
  }

  override def afterAll {
    sql("use default")
    sql("drop table if exists columnComment")
    sql("drop table if exists defaultComment")
  }

}

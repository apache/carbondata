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
 * test functionality related the case change for database name
 */
class TestCreateTableWithDatabaseNameCaseChange extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("use default")
    sql("drop database if exists dbCaseChange cascade")
  }

  test("test create table with database case name change") {
    // this test case will test the creation of table for different case for database name.
    // In hive dbName folder is always created with small case in HDFS. Carbon should behave
    // the same way. If table creation fails during second time creation it means in HDFS
    // separate folders are created for the matching case in commands executed.
    sql("create database dbCaseChange")
    sql("use DBCaseChanGe")
    sql("create table carbonTable(a int, b string)STORED AS carbondata")
    sql("drop table carbonTable")
    sql("use default")
    sql("use dbcasechange")
    try {
      // table creation should be successful
      sql("create table carbonTable(a int, b string)STORED AS carbondata")
      assert(true)
    } catch {
      case ex: Exception =>
        assert(false)
    }
  }

  test("test drop database cascade with case sensitive") {
    // this test case will test the creation of table for different case for database name.
    // In hive dbName folder is always created with small case in HDFS. Carbon should behave
    // the same way. If table creation fails during second time creation it means in HDFS
    // separate folders are created for the matching case in commands executed.
    sql("drop database if exists AbCdEf cascade")
    sql("create database AbCdEf")
    sql("use AbCdEf")
    sql("create table carbonTable(a int, b string)STORED AS carbondata")
    sql("use default")
    sql("drop database if exists AbCdEf cascade")
    sql("create database AbCdEf")
    sql("use AbCdEf")
    try {
      sql("create table carbonTable(a int, b string)STORED AS carbondata")
      assert(true)
    } catch {
      case ex: Exception =>
        assert(false)
    }
    sql("use default")
    sql("drop database if exists AbCdEf cascade")
  }

  override def afterAll {
    sql("use default")
    sql("drop database if exists dbCaseChange cascade")
  }
}

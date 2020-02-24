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
package org.apache.carbondata.spark.testsuite.deleteTable

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * test class for testing the create cube DDL.
 */
class TestDeleteTableNewDDL extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("drop table if exists CaseInsensitiveTable")
    sql("drop table if exists dropTableTest1")
    sql("drop table if exists dropTableTest2")
    sql("drop table if exists dropTableTest4")
    sql("drop table if exists table1")
    sql("drop table if exists table2")

    sql("CREATE TABLE IF NOT EXISTS table1(empno Int, empname Array<String>, designation String, doj Timestamp, "
        + "workgroupcategory Int, workgroupcategoryname String, deptno Int, deptname String, projectcode Int, "
        + "projectjoindate Timestamp, projectenddate Timestamp , attendance Int,utilization Int,salary Int )"
        + " STORED AS carbondata ")
    sql("CREATE TABLE IF NOT EXISTS table2(empno Int, empname Array<String>, designation String, doj Timestamp, "
        + "workgroupcategory Int, workgroupcategoryname String, deptno Int, deptname String, projectcode Int, "
        + "projectjoindate Timestamp, projectenddate Timestamp , attendance Int,utilization Int,salary Int )"
        + " STORED AS carbondata ")

  }

  // normal deletion case
  test("drop table Test with new DDL") {
    sql("drop table table1")
  }

  test("test drop database") {
    var dbName = "dropdb_test"
    sql(s"drop database if exists $dbName cascade")
    sql(s"create database $dbName")
    sql(s"drop database $dbName")
    assert(intercept[Exception] {
      sql(s"use $dbName")
    }.getMessage.contains("Database 'dropdb_test' not found"))
  }

  test("test drop database cascade command") {
    sql("drop database if exists testdb cascade")
    sql("create database testdb")
    sql("use testdb")
    sql("CREATE TABLE IF NOT EXISTS testtable(empno Int, empname string, utilization Int,salary Int)"
        + " STORED AS carbondata ")
    intercept[Exception] {
      sql("drop database testdb")
    }
    sql("drop database testdb cascade")
    intercept[Exception] {
      sql("use testdb")
    }
    sql("use default")
  }

  // deletion case with if exists
  test("drop table if exists Test with new DDL") {
    sql("drop table if exists table2")

  }

  // try to delete after deletion with if exists
  test("drop table after deletion with if exists with new DDL") {
    sql("drop table if exists table2")

  }

  // try to delete after deletion with out if exists. this should fail
  test("drop table after deletion with new DDL") {
    try {
      sql("drop table table2")
      fail("failed") // this should not be executed as exception is expected
    }
    catch {
      case _: Exception => // pass the test case as this is expected
    }


  }

  test("drop table using case insensitive table name") {
    sql("drop table if exists CaseInsensitiveTable")
    // create table
    sql(
      "CREATE table CaseInsensitiveTable (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) STORED AS carbondata"
    )
    // table should drop wihout any error
    sql("drop table caseInsensitiveTable")

    // Now create same table, it should not give any error.
    sql(
      "CREATE table CaseInsensitiveTable (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) STORED AS carbondata"
    )

  }

  test("drop table using dbName and table name") {
    // create table
    sql(
      "CREATE table default.table3 (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) STORED AS carbondata"
    )
    // table should drop without any error
    sql("drop table default.table3")
  }


  test("drop table and create table with different data type") {
    sql("drop table if exists droptabletest1")
    sql(
      "CREATE table dropTableTest1 (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) STORED AS carbondata "

    )

    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE dropTableTest1 " +
      "OPTIONS('DELIMITER' =  ',')")
    sql("drop table dropTableTest1")

    sql(
      "CREATE table dropTableTest1 (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary String) STORED AS carbondata "
    )

    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE dropTableTest1 " +
      "OPTIONS('DELIMITER' =  ',')")

  }


  test("drop table and create table with dictionary exclude integer scenario") {
    sql("drop table if exists dropTableTest2")
    sql(
      "CREATE table dropTableTest2 (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) STORED AS carbondata "
    )
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE dropTableTest2 " +
      "OPTIONS('DELIMITER' =  ',')")
    sql("drop table dropTableTest2")
    sql(
      "CREATE table dropTableTest2 (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary decimal) STORED AS carbondata "
    )
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE dropTableTest2 " +
      "OPTIONS('DELIMITER' =  ',')")

  }

  test("drop table and create table with dictionary exclude string scenario") {
    try {

      sql("create database test")
      sql(
        "CREATE table test.dropTableTest3 (ID int, date String, country String, name " +
        "String," +
        "phonetype String, serialname String, salary int) STORED AS carbondata "
      )
      sql(
        s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE test.dropTableTest3 " +
        "OPTIONS('DELIMITER' =  ',')")
      sql("drop table test.dropTableTest3")
      sql(
        "CREATE table test.dropTableTest3 (ID int, date String, country String, name " +
        "String," +
        "phonetype String, serialname String, salary decimal) STORED AS carbondata "
      )
      sql(
        s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE test.dropTableTest3 " +
        "OPTIONS('DELIMITER' =  ',')")
    } finally {
      sql("drop table test.dropTableTest3")
      sql("drop database test")
    }

  }

  test("drop table and create table with same name but different cols") {
    sql("drop table if exists dropTableTest4")
    sql(
      "CREATE TABLE dropTableTest4 (imei string,age int,task bigint,name string,country string," +
      "city string,sale int,num double,level decimal(10,3),quest bigint,productdate timestamp," +
      "enddate timestamp,PointId double,score decimal(10,3))STORED AS carbondata")
    sql(
      s"LOAD DATA INPATH '$resourcesPath/big_int_Decimal.csv'  INTO TABLE dropTableTest4 " +
      "options ('DELIMITER'=',', 'QUOTECHAR'='\"', 'COMPLEX_DELIMITER_LEVEL_1'='$'," +
      "'COMPLEX_DELIMITER_LEVEL_2'=':', 'FILEHEADER'= '')")
    sql("drop table dropTableTest4")
    sql(
      "CREATE table dropTableTest4 (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary decimal) STORED AS carbondata "
    )
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE dropTableTest4 " +
      "OPTIONS('DELIMITER' =  ',')")

  }


  override def afterAll: Unit = {
    sql("use default")
    sql("drop table if exists CaseInsensitiveTable")
    sql("drop table if exists dropTableTest1")
    sql("drop table if exists dropTableTest2")
    sql("drop table if exists dropTableTest4")
    sql("drop table if exists table1")
    sql("drop table if exists table2")
    sql("drop database if exists test cascade")
  }

}

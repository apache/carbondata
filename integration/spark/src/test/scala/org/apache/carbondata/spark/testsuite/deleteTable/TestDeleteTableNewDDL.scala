/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.spark.testsuite.deleteTable

import java.io.File

import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * test class for testing the create cube DDL.
 */
class TestDeleteTableNewDDL extends QueryTest with BeforeAndAfterAll {

  val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
    .getCanonicalPath
  val resource = currentDirectory + "/src/test/resources/"

  override def beforeAll: Unit = {

    sql("CREATE TABLE IF NOT EXISTS table1(empno Int, empname Array<String>, designation String, doj Timestamp, "
        + "workgroupcategory Int, workgroupcategoryname String, deptno Int, deptname String, projectcode Int, "
        + "projectjoindate Timestamp, projectenddate Timestamp , attendance Int,utilization Int,salary Int )"
        + " STORED BY 'org.apache.carbondata.format' ")
    sql("CREATE TABLE IF NOT EXISTS table2(empno Int, empname Array<String>, designation String, doj Timestamp, "
        + "workgroupcategory Int, workgroupcategoryname String, deptno Int, deptname String, projectcode Int, "
        + "projectjoindate Timestamp, projectenddate Timestamp , attendance Int,utilization Int,salary Int )"
        + " STORED BY 'org.apache.carbondata.format' ")

  }

  // normal deletion case
  test("drop table Test with new DDL") {
    sql("drop table table1")

  }
  
  test("test drop database cascade command") {
    sql("create database testdb")
    try {
      sql("drop database testdb cascade")
      assert(false)
    } catch {
      case e : MalformedCarbonCommandException => {
        assert(e.getMessage.equals("Unsupported cascade operation in drop database/schema command"))
      }
    }
    sql("drop database testdb")
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
      case e: Exception => // pass the test case as this is expected
    }


  }

  test("drop table using case insensitive table name") {
    // create table
    sql(
      "CREATE table CaseInsensitiveTable (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) stored by 'org.apache.carbondata.format'" +
      "TBLPROPERTIES('DICTIONARY_INCLUDE'='ID', 'DICTIONARY_INCLUDE'='salary')"
    )
    // table should drop wihout any error
    sql("drop table caseInsensitiveTable")

    // Now create same table, it should not give any error.
    sql(
      "CREATE table CaseInsensitiveTable (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) stored by 'org.apache.carbondata.format'" +
      "TBLPROPERTIES('DICTIONARY_INCLUDE'='ID', 'DICTIONARY_INCLUDE'='salary')"
    )

  }

  test("drop table using dbName and table name") {
    // create table
    sql(
      "CREATE table default.table3 (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) stored by 'org.apache.carbondata.format'" +
      "TBLPROPERTIES('DICTIONARY_INCLUDE'='ID', 'DICTIONARY_INCLUDE'='salary')"
    )
    // table should drop without any error
    sql("drop table default.table3")
  }


  test("drop table and create table with different data type") {
    sql(
      "CREATE table dropTableTest1 (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) stored by 'org.apache.carbondata.format' "

    )

    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE dropTableTest1 " +
      "OPTIONS('DELIMITER' =  ',')")
    sql("select * from dropTableTest1")
    sql("drop table dropTableTest1")

    sql(
      "CREATE table dropTableTest1 (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary String) stored by 'org.apache.carbondata.format' "
    )

    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE dropTableTest1 " +
      "OPTIONS('DELIMITER' =  ',')")

    sql("select * from dropTableTest1")

  }


  test("drop table and create table with dictionary exclude integer scenario") {
    sql(
      "CREATE table dropTableTest2 (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) stored by 'org.apache.carbondata.format' " +
      "TBLPROPERTIES('DICTIONARY_EXCLUDE'='salary')"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE dropTableTest2 " +
      "OPTIONS('DELIMITER' =  ',')")
    sql("select * from dropTableTest2")
    sql("drop table dropTableTest2")
    sql(
      "CREATE table dropTableTest2 (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary decimal) stored by 'org.apache.carbondata.format' " +
      "TBLPROPERTIES('DICTIONARY_EXCLUDE'='date')"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE dropTableTest2 " +
      "OPTIONS('DELIMITER' =  ',')")
    sql("select * from dropTableTest2")

  }

  test("drop table and create table with dictionary exclude string scenario") {
    sql("create database if not exists test")
    sql(
      "CREATE table test.dropTableTest3 (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) stored by 'org.apache.carbondata.format' " +
      "TBLPROPERTIES('DICTIONARY_EXCLUDE'='salary')"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE test.dropTableTest3 " +
      "OPTIONS('DELIMITER' =  ',')")
    sql("select * from test.dropTableTest3")
    sql("drop table test.dropTableTest3")
    sql(
      "CREATE table test.dropTableTest3 (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary decimal) stored by 'org.apache.carbondata.format' " +
      "TBLPROPERTIES('DICTIONARY_EXCLUDE'='date')"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE test.dropTableTest3 " +
      "OPTIONS('DELIMITER' =  ',')")
    sql("select * from test.dropTableTest3")

  }

  test("drop table and create table with same name but different cols") {

    sql(
      "CREATE TABLE dropTableTest4 (imei string,age int,task bigint,name string,country string," +
      "city string,sale int,num double,level decimal(10,3),quest bigint,productdate timestamp," +
      "enddate timestamp,PointId double,score decimal(10,3))STORED BY 'org.apache.carbondata" +
      ".format'")
    sql(
      "LOAD DATA INPATH './src/test/resources/big_int_Decimal.csv'  INTO TABLE dropTableTest4 " +
      "options ('DELIMITER'=',', 'QUOTECHAR'='\"', 'COMPLEX_DELIMITER_LEVEL_1'='$'," +
      "'COMPLEX_DELIMITER_LEVEL_2'=':', 'FILEHEADER'= '')")
    sql("select * from dropTableTest4")
    sql("drop table dropTableTest4")
    sql(
      "CREATE table dropTableTest4 (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary decimal) stored by 'org.apache.carbondata" +
      ".format' " +
      "TBLPROPERTIES('DICTIONARY_EXCLUDE'='date')"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE dropTableTest4 " +
      "OPTIONS('DELIMITER' =  ',')")
    sql("select * from dropTableTest4")


  }


  override def afterAll: Unit = {

    sql("drop table CaseInsensitiveTable")
    sql("drop table dropTableTest1")
    sql("drop table dropTableTest2")
    sql("drop table test.dropTableTest3")
    sql("drop database test")
    sql("drop table dropTableTest4")
  }

}

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
package org.carbondata.spark.testsuite.deleteTable

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * test class for testing the create cube DDL.
 */
class TestDeleteTableNewDDL extends QueryTest with BeforeAndAfterAll {

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

  override def afterAll: Unit = {

    sql("drop table CaseSensitiveTable")
  }

}

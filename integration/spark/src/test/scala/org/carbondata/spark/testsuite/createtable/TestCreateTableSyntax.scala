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

package org.carbondata.spark.testsuite.createtable

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest

import org.carbondata.spark.exception.MalformedCarbonCommandException

import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for validating create table syntax for carbontable
 *
 */
class TestCreateTableSyntax extends QueryTest with BeforeAndAfterAll {
  
  override def beforeAll {
    sql("drop table if exists carbontable")
  }

  test("Struct field with underscore and struct<struct> syntax check") {
    sql("create table carbontable(id int, username struct<sur_name:string," +
        "actual_name:struct<first_name:string,last_name:string>>, country string, salary double)" +
        "STORED BY 'org.apache.carbondata.format'")
    sql("describe carbontable").show
    sql("drop table if exists carbontable")
  }
  
  test("test carbon table create with complex datatype as dictionary exclude") {
    try {
      sql("create table carbontable(id int, name string, dept string, mobile array<string>, "+
          "country string, salary double) STORED BY 'org.apache.carbondata.format' " +
          "TBLPROPERTIES('DICTIONARY_EXCLUDE'='dept,mobile')")
      assert(false)
    } catch {
      case e : MalformedCarbonCommandException => {
        assert(e.getMessage.equals("DICTIONARY_EXCLUDE is unsupported for complex datatype column: mobile"))
      }
    }
    sql("drop table if exists carbontable")
  }

  test("test carbon table create with double datatype as dictionary exclude") {
    try {
      sql("create table carbontable(id int, name string, dept string, mobile array<string>, "+
        "country string, salary double) STORED BY 'org.apache.carbondata.format' " +
        "TBLPROPERTIES('DICTIONARY_EXCLUDE'='salary')")
      assert(false)
    } catch {
      case e : MalformedCarbonCommandException => {
        assert(e.getMessage.equals("DICTIONARY_EXCLUDE is unsupported for double " +
          "data type column: salary"))
      }
    }
    sql("drop table if exists carbontable")
  }

  test("test carbon table create with decimal datatype as dictionary exclude") {
    try {
      sql("create table carbontable(id int, name string, dept string, mobile array<string>, "+
        "country string, salary decimal) STORED BY 'org.apache.carbondata.format' " +
        "TBLPROPERTIES('DICTIONARY_EXCLUDE'='salary')")
      assert(false)
    } catch {
      case e : MalformedCarbonCommandException => {
        assert(e.getMessage.equals("DICTIONARY_EXCLUDE is unsupported for decimal " +
          "data type column: salary"))
      }
    }
  }
  
  test("describe formatted on hive table and carbon table") {
    sql("create table carbontable(id int, username struct<sur_name:string," +
        "actual_name:struct<first_name:string,last_name:string>>, country string, salary double)" +
        "STORED BY 'org.apache.carbondata.format'")
    sql("describe formatted carbontable").show(50)
    sql("drop table if exists carbontable")
    sql("create table hivetable(id int, username struct<sur_name:string," +
        "actual_name:struct<first_name:string,last_name:string>>, country string, salary double)")
    sql("describe formatted hivetable").show(50)
    sql("drop table if exists hivetable")
  }

  override def afterAll {
    sql("drop table if exists carbontable")
  }
}
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

package org.apache.carbondata.spark.testsuite.createtable

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

/**
 * Test Class for validating create table syntax for carbontable
 *
 */
class TestCreateTableSyntax extends QueryTest with BeforeAndAfterAll {
  
  override def beforeAll {
  }

  test("Struct field with underscore and struct<struct> syntax check") {
    sql("drop table if exists carbontable")
    sql("create table carbontable(id int, username struct<sur_name:string," +
        "actual_name:struct<first_name:string,last_name:string>>, country string, salary double)" +
        "STORED BY 'org.apache.carbondata.format'")
    sql("describe carbontable")
  }
  
  test("Test table rename operation on carbon table and on hive table") {
    sql("drop table if exists hivetable")
    sql("drop table if exists carbontable")
    sql("drop table if exists hiveRenamedTable")
    sql("drop table if exists carbonRenamedTable")
    sql("create table hivetable(test1 int, test2 array<String>,test3 array<bigint>,"+
        "test4 array<int>,test5 array<decimal>,test6 array<timestamp>,test7 array<double>)"+
        "row format delimited fields terminated by ',' collection items terminated by '$' map keys terminated by ':'")
    sql("alter table hivetable rename To hiveRenamedTable")
    sql("create table carbontable(test1 int, test2 array<String>,test3 array<bigint>,"+
        "test4 array<int>,test5 array<decimal>,test6 array<timestamp>,test7 array<double>)"+
        "STORED BY 'org.apache.carbondata.format'")
    sql("alter table carbontable compact 'minor'")
    try {
      sql("alter table carbontable rename To carbonRenamedTable")
      assert(false)
    } catch {
      case e : MalformedCarbonCommandException => {
        assert(e.getMessage.equals("Unsupported alter operation on carbon table"))
      }
    }
  }

  
  test("test carbon table create with complex datatype as dictionary exclude") {
    try {
      sql("drop table if exists carbontable")
      sql("create table carbontable(id int, name string, dept string, mobile array<string>, "+
          "country string, salary double) STORED BY 'org.apache.carbondata.format' " +
          "TBLPROPERTIES('DICTIONARY_EXCLUDE'='dept,mobile')")
      assert(false)
    } catch {
      case e : MalformedCarbonCommandException => {
        assert(e.getMessage.equals("DICTIONARY_EXCLUDE is unsupported for complex datatype column: mobile"))
      }
    }
  }

  test("test carbon table create with double datatype as dictionary exclude") {
    try {
      sql("drop table if exists carbontable")
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
  }
    test("test carbon table create with int datatype as dictionary exclude") {
    try {
      sql("drop table if exists carbontable")
      sql("create table carbontable(id int, name string, dept string, mobile array<string>, "+
        "country string, salary double) STORED BY 'org.apache.carbondata.format' " +
        "TBLPROPERTIES('DICTIONARY_EXCLUDE'='id')")
      assert(false)
    } catch {
      case e : MalformedCarbonCommandException => {
        assert(e.getMessage.equals("DICTIONARY_EXCLUDE is unsupported for int " +
          "data type column: id"))
      }
    }
  }

  test("test carbon table create with decimal datatype as dictionary exclude") {
    try {
      sql("drop table if exists carbontable")
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
    sql("drop table if exists hivetable")
    sql("drop table if exists carbontable")
    sql("create table carbontable(id int, username struct<sur_name:string," +
        "actual_name:struct<first_name:string,last_name:string>>, country string, salary double)" +
        "STORED BY 'org.apache.carbondata.format'")
    sql("describe formatted carbontable")
    sql("create table hivetable(id int, username struct<sur_name:string," +
        "actual_name:struct<first_name:string,last_name:string>>, country string, salary double)")
    sql("describe formatted hivetable")
  }

  test("describe command carbon table for decimal scale and precision test") {
    sql("drop table if exists carbontablePrecision")
    sql("create table carbontablePrecision(id int, name string, dept string, mobile array<string>, "+
        "country string, salary decimal(10,6)) STORED BY 'org.apache.carbondata.format' " +
        "TBLPROPERTIES('DICTIONARY_INCLUDE'='salary,id')")
    checkAnswer(
      sql("describe carbontablePrecision"),
      Seq(Row("country","string",""),
        Row("dept","string",""),Row("id","int",""),Row("mobile","array<string>",""),Row("name","string",""),
        Row("salary","decimal(10,6)","")
      )
    )
  }
  
  test("create carbon table without dimensions") {
    try {
      sql("drop table if exists carbontable")
      sql("create table carbontable(msr1 int, msr2 double, msr3 bigint, msr4 decimal)" +
        " stored by 'org.apache.carbondata.format'")
      assert(false)
    } catch {
      case e : MalformedCarbonCommandException => {
        assert(e.getMessage.equals("Table default.carbontable can not be created without " +
          "key columns. Please use DICTIONARY_INCLUDE or DICTIONARY_EXCLUDE to " +
          "set at least one key column if all specified columns are numeric types"))
      }
    }
  }

  test("create carbon table with repeated table properties") {
    try {
      sql("drop table if exists carbontable")
      sql(
        """
          CREATE TABLE IF NOT EXISTS carbontable
          (ID Int, date Timestamp, country String,
          name String, phonetype String, serialname String, salary Int)
          STORED BY 'carbondata'
          TBLPROPERTIES('DICTIONARY_EXCLUDE'='country','DICTIONARY_INCLUDE'='ID',
          'DICTIONARY_EXCLUDE'='phonetype', 'DICTIONARY_INCLUDE'='salary')
        """)
      assert(false)
    } catch {
      case e : MalformedCarbonCommandException => {
        assert(e.getMessage.equals("Table properties is repeated: dictionary_include,dictionary_exclude"))
      }
    }
  }

  override def afterAll {
    sql("drop table if exists hivetable")
    sql("drop table if exists carbontable")
    sql("drop table if exists hiveRenamedTable")
    sql("drop table if exists carbonRenamedTable")
    sql("drop table if exists carbontablePrecision")
  }
}
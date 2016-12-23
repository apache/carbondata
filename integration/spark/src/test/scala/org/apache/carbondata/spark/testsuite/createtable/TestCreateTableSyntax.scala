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

package org.apache.carbondata.spark.testsuite.createtable

import org.apache.carbondata.core.carbon.metadata.encoder.Encoding
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.{CarbonContext, Row}
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
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
  
  test("Test table rename operation on carbon table and on hive table") {
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
    sql("drop table if exists hiveRenamedTable")
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
    test("test carbon table create with int datatype as dictionary exclude") {
    try {
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

    test("describe command carbon table for decimal scale and precision test") {
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
     sql("drop table if exists carbontablePrecision")
  }
  
  test("create carbon table without dimensions") {
    try {
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
  test("Create carbon table as select from other table") {
    try {
      // create and load into hive table
      sql("CREATE TABLE IF NOT EXISTS otherHiveTable (ID Int, date Timestamp, country String," +
        "name String, phonetype String, serialname String, salary Int) " +
        "row format delimited fields terminated by ','")
      sql("LOAD DATA LOCAL INPATH './src/test/resources/data2.csv' INTO TABLE " +
        "otherHiveTable")
      // create and load into carbon table
      sql("CREATE TABLE IF NOT EXISTS otherCarbonTable (ID Int, date Timestamp, country String," +
        "name String, phonetype String, serialname String, salary Int) " +
        "STORED BY 'carbondata'")
      sql("LOAD DATA LOCAL INPATH './src/test/resources/data2.csv' INTO TABLE " +
        "otherCarbonTable")
      // create as select from hive table
      sql("CREATE TABLE IF NOT EXISTS carbonFromHive STORED BY 'carbondata' as SELECT * FROM otherHiveTable")
      // create as select from carbon table
      sql("CREATE TABLE IF NOT EXISTS carbonFromCarbon STORED BY 'carbondata' as SELECT * FROM otherCarbonTable")
      // test when table name contains keywords 'select'
      sql("CREATE TABLE IF NOT EXISTS selectTable STORED BY 'carbondata' as SELECT * FROM OtherHiveTable")
      // test create as select with table properties
      sql("CREATE TABLE IF NOT EXISTS carbonWithTblpro STORED BY 'carbondata'" +
        " TBLPROPERTIES('DICTIONARY_INCLUDE'='ID','DICTIONARY_EXCLUDE'='phonetype') " +
        "as SELECT * FROM OtherHiveTable")
      // compare with original table
      checkAnswer(sql("SELECT salary FROM carbonFromHive order by phonetype"),
        sql("SELECT salary FROM otherHiveTable order by phonetype"))
      checkAnswer(sql("SELECT salary FROM carbonFromCarbon order by phonetype"),
        sql("SELECT salary FROM otherCarbonTable order by phonetype"))
      // check whether tableproeprties works
      val carbonTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
        .getCarbonTable("default" + CarbonCommonConstants.UNDERSCORE + "carbonWithtblpro")
      assert(carbonTable.getDimensionByName("carbonwithtblpro", "id").
        hasEncoding(Encoding.DICTIONARY))
      assert(!carbonTable.getDimensionByName("carbonwithtblpro", "phonetype").
        hasEncoding(Encoding.DICTIONARY))
      sql("drop table if exists hivetable")
      sql("drop table if exists otherCarbonTable")
      sql("drop table if exists carbonFromHive")
      sql("drop table if exists carbonFromCarbon")
      sql("drop table if exists selectTable")
      sql("drop table if exists carbonwithtblpro")

    } catch {
      case ex: Exception =>
        throw ex
        assert(false)
    }
  }

  override def afterAll {
    sql("drop table if exists carbontable")
  }
}
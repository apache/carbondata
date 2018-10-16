/*

    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements. See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License. You may obtain a copy of the License at
    *
    http://www.apache.org/licenses/LICENSE-2.0
    *
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
    */
package org.apache.carbondata.spark.testsuite.createTable.TestCreateDDLForComplexMapType

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestCreateDDLForComplexMapType extends QueryTest with BeforeAndAfterAll {
  private val conf: Configuration = new Configuration(false)

  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath

  val path = s"$rootPath/examples/spark2/src/main/resources/mapDDLTestData.csv"

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS carbon")
  }

  test("Single Map One Level") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<STRING,STRING>
         | )
         | STORED BY 'carbondata'
         | """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    assert(desc(0).get(1).asInstanceOf[String].trim.equals("map<string,string>"))
  }

  test("Single Map with Two Nested Level") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<STRING,map<INT,STRING>>
         | )
         | STORED BY
         |'carbondata'
         |"""
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    assert(desc(0).get(1).asInstanceOf[String].trim.equals("map<string,map<int,string>>"))
  }

  test("Map Type with array type as value") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<STRING,array<INT>>
         | )
         | STORED BY 'carbondata'
         |
         """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    assert(desc(0).get(1).asInstanceOf[String].trim.equals("map<string,array<int>>"))
  }

  test("Map Type with struct type as value") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<STRING,struct<key:INT,val:INT>>
         | )
         | STORED BY
         | 'carbondata'
         | """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    assert(desc(0).get(1).asInstanceOf[String].trim
      .equals("map<string,struct<key:int,val:int>>"))
  }

  test("Map Type as child to struct type") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField struct<key:INT,val:map<INT,INT>>
         | )
         | STORED BY
         |'carbondata' """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    assert(desc(0).get(1).asInstanceOf[String].trim
      .equals("struct<key:int,val:map<int,int>>"))
  }

  test("Map Type as child to array type") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField array<map<INT,INT>>
         | )
         | STORED BY 'carbondata'
         | """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon """.stripMargin).collect()
    assert(desc(0).get(1).asInstanceOf[String].trim.equals("array<map<int,int>>"))
    sql("insert into carbon values('1\0032:2\0033$100\003200:200\003300')")
    sql("select * from carbon").show(false)
  }

  test("Map Type as child to array<array> type which should not work") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField array<array<map<INT,INT>>>
         | )
         | STORED BY
         |'carbondata'
         |"""
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon """.stripMargin).collect()
    assert(desc(0).get(1).asInstanceOf[String].trim.equals("array<array<map<int,int>>>"))
    val exception1 = intercept[Exception] {
      sql("insert into carbon values('1\0042\0032\0043:100\004200\003200\004300$1\0042\0032\0043" +
          ":100\004200\003200\004300')")
    }
    assert(exception1.getMessage.contains("DataLoad failure"))
  }

  test("Test Load data in map") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<INT,STRING>
         | )
         | STORED BY 'carbondata'
         | """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    sql("insert into carbon values('1:Nalla$2:Singh$3:Gupta$4:Kumar')")
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map(1 -> "Nalla", 2 -> "Singh", 3 -> "Gupta", 4 -> "Kumar"))))
  }

  test("Test Load duplicate keys data in map") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<INT,STRING>
         | )
         | STORED BY 'carbondata'
         | """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    sql("insert into carbon values('1:Nalla$2:Singh$1:Gupta$4:Kumar')")
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map(1 -> "Nalla", 2 -> "Singh", 4 -> "Kumar"))))
  }

  test("Test Load data in map of map") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<STRING,map<INT,STRING>>
         | )
         | STORED BY
         |'carbondata' """
        .stripMargin)
    sql(
      "insert into carbon values('manish:1\004nalla\0032\004gupta$kunal:1\004kapoor\0032\004sharma')")
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map("manish" -> Map(1 -> "nalla", 2 -> "gupta"),
        "kunal" -> Map(1 -> "kapoor", 2 -> "sharma")))))
  }

  test("Test Load duplicate keys data in map of map") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<STRING,map<INT,STRING>>
         | )
         | STORED BY
         |'carbondata'
         |"""
        .stripMargin)
    sql(
      "insert into carbon values('manish:1\004nalla\0031\004gupta$kunal:1\004kapoor\0032\004sharma')")
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map("manish" -> Map(1 -> "nalla"),
        "kunal" -> Map(1 -> "kapoor", 2 -> "sharma")))))
  }

  test("Test Create table as select with map") {
    sql("DROP TABLE IF EXISTS carbon")
    sql("DROP TABLE IF EXISTS carbon1")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<INT,STRING>
         | )
         | STORED BY 'carbondata'
         | """
        .stripMargin)
    sql("insert into carbon values('1:Nalla$2:Singh$3:Gupta$4:Kumar')")
    sql(
      s"""
         | CREATE TABLE carbon1
         | AS
         | Select *
         | From carbon
         | """
        .stripMargin)
    checkAnswer(sql("select * from carbon1"), Seq(
      Row(Map(1 -> "Nalla", 2 -> "Singh", 3 -> "Gupta", 4 -> "Kumar"))))
  }

  test("Test Create table with double datatype in map") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<DOUBLE,STRING>
         | )
         | STORED BY 'carbondata'
         | """
        .stripMargin)
    sql("insert into carbon values('1.23:Nalla$2.34:Singh$3.67676:Gupta$3.67676:Kumar')")
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map(1.23 -> "Nalla", 2.34 -> "Singh", 3.67676 -> "Gupta"))))
  }

  test("Load Map data from CSV File") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<INT,STRING>
         | )
         | STORED BY 'carbondata'
         | """
        .stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon
         | OPTIONS('header'='false')
       """.stripMargin)
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map(1 -> "Nalla", 2 -> "Singh", 4 -> "Kumar")),
      Row(Map(10 -> "Nallaa", 20 -> "Sissngh", 100 -> "Gusspta", 40 -> "Kumar"))
    ))
  }

  test("Sort Column table property blocking for Map type") {
    sql("DROP TABLE IF EXISTS carbon")
    val exception1 = intercept[Exception] {
      sql(
        s"""
           | CREATE TABLE carbon(
           | mapField map<STRING,STRING>
           | )
           | STORED BY 'carbondata'
           | TBLPROPERTIES('SORT_COLUMNS'='mapField')
           | """
          .stripMargin)
    }
    assert(exception1.getMessage
      .contains(
        "sort_columns is unsupported for map datatype column: mapfield"))
  }

}
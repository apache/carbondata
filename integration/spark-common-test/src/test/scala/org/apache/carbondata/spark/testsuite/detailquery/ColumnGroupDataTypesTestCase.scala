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

package org.apache.carbondata.spark.testsuite.detailquery

import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for aggregate query on multiple datatypes
 *
 */
class ColumnGroupDataTypesTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists colgrp")
    sql("drop table if exists normal")
    sql("drop table if exists colgrp_dictexclude_before")
    sql("drop table if exists colgrp_dictexclude_after")
    sql("drop table if exists colgrp_disorder")

    sql("create table colgrp (column1 string,column2 string,column3 string,column4 string,column5 string,column6 string,column7 string,column8 string,column9 string,column10 string,measure1 int,measure2 int,measure3 int,measure4 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES (\"COLUMN_GROUPS\"=\"(column2,column3,column4),(column7,column8,column9)\")")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/10dim_4msr.csv' INTO table colgrp options('FILEHEADER'='column1,column2,column3,column4,column5,column6,column7,column8,column9,column10,measure1,measure2,measure3,measure4')");
    sql("create table normal (column1 string,column2 string,column3 string,column4 string,column5 string,column6 string,column7 string,column8 string,column9 string,column10 string,measure1 int,measure2 int,measure3 int,measure4 int) STORED BY 'org.apache.carbondata.format'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/10dim_4msr.csv' INTO table normal options('FILEHEADER'='column1,column2,column3,column4,column5,column6,column7,column8,column9,column10,measure1,measure2,measure3,measure4')");
    //column group with dictionary exclude before column group
    sql("create table colgrp_dictexclude_before (column1 string,column2 string,column3 string,column4 string,column5 string,column6 string,column7 string,column8 string,column9 string,column10 string,measure1 int,measure2 int,measure3 int,measure4 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ('DICTIONARY_EXCLUDE'='column1',\"COLUMN_GROUPS\"=\"(column2,column3,column4),(column7,column8,column9)\")")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/10dim_4msr.csv' INTO table colgrp_dictexclude_before options('FILEHEADER'='column1,column2,column3,column4,column5,column6,column7,column8,column9,column10,measure1,measure2,measure3,measure4')");
    //column group with dictionary exclude after column group
    sql("create table colgrp_dictexclude_after (column1 string,column2 string,column3 string,column4 string,column5 string,column6 string,column7 string,column8 string,column9 string,column10 string,measure1 int,measure2 int,measure3 int,measure4 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ('DICTIONARY_EXCLUDE'='column10',\"COLUMN_GROUPS\"=\"(column2,column3,column4),(column7,column8,column9)\")")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/10dim_4msr.csv' INTO table colgrp_dictexclude_after options('FILEHEADER'='column1,column2,column3,column4,column5,column6,column7,column8,column9,column10,measure1,measure2,measure3,measure4')");
    
  }

  test("select all dimension query") {
    checkAnswer(
      sql("select column1,column2,column3,column4,column5,column6,column7,column8,column9,column10 from colgrp"),
      sql("select column1,column2,column3,column4,column5,column6,column7,column8,column9,column10 from normal"))
  }

  test("select all dimension query with filter on columnar") {
    checkAnswer(
      sql("select column1,column2,column3,column4,column5,column6,column7,column8,column9,column10 from colgrp where column1='column1666'"),
      sql("select column1,column2,column3,column4,column5,column6,column7,column8,column9,column10 from normal where column1='column1666'"))
  }

  test("select all dimension query with filter on column group dimension") {
    checkAnswer(
      sql("select column1,column2,column3,column4,column5,column6,column7,column8,column9,column10 from colgrp where column3='column311'"),
      sql("select column1,column2,column3,column4,column5,column6,column7,column8,column9,column10 from normal where column3='column311'"))
  }

  test("select all dimension query with filter on two dimension from different column group") {
    checkAnswer(
      sql("select column1,column2,column3,column4,column5,column6,column7,column8,column9,column10 from colgrp where column3='column311' and column7='column74' "),
      sql("select column1,column2,column3,column4,column5,column6,column7,column8,column9,column10 from normal where column3='column311' and column7='column74'"))
  }

  test("select all dimension query with filter on two dimension from same column group") {
    checkAnswer(
      sql("select column1,column2,column3,column4,column5,column6,column7,column8,column9,column10 from colgrp where column3='column311' and column4='column42' "),
      sql("select column1,column2,column3,column4,column5,column6,column7,column8,column9,column10 from normal where column3='column311' and column4='column42'"))
  }

  test("select all dimension query with filter on two dimension one from column group other from columnar") {
    checkAnswer(
      sql("select column1,column2,column3,column4,column5,column6,column7,column8,column9,column10 from colgrp where column3='column311' and column5='column516' "),
      sql("select column1,column2,column3,column4,column5,column6,column7,column8,column9,column10 from normal where column3='column311' and column5='column516'"))
  }

  test("select few dimension") {
    checkAnswer(
      sql("select column1,column3,column4,column5,column6,column9,column10 from colgrp"),
      sql("select column1,column3,column4,column5,column6,column9,column10 from normal"))
  }

  test("select count on column group") {
    checkAnswer(
      sql("select count(column2) from colgrp"),
      sql("select count(column2) from normal"))
  }
   test("##ColumnGroup_DictionaryExcludeBefore select all dimension on column group and dictionary exclude table") {
    checkAnswer(
      sql("select * from colgrp_dictexclude_before"),
      sql("select * from normal"))
  }
  test("##ColumnGroup_DictionaryExcludeBefore select all dimension query with filter on two dimension from same column group") {
    checkAnswer(
      sql("select * from colgrp_dictexclude_before where column3='column311' and column4='column42' "),
      sql("select * from normal where column3='column311' and column4='column42'"))
  }
  test("##ColumnGroup_DictionaryExcludeAfter select all dimension on column group and dictionary exclude table") {
    checkAnswer(
      sql("select * from colgrp_dictexclude_after"),
      sql("select * from normal"))
  }
  test("##ColumnGroup_DictionaryExcludeAfter select all dimension query with filter on two dimension from same column group") {
    checkAnswer(
      sql("select * from colgrp_dictexclude_after where column3='column311' and column4='column42' "),
      sql("select * from normal where column3='column311' and column4='column42'"))
  }
  test("ExcludeFilter") {
    checkAnswer(
      sql("select * from colgrp where column3 != 'column311'"),
      sql("select * from normal where column3 != 'column311'"))

    checkAnswer(
      sql("select * from colgrp where column3 like 'column31%'"),
      sql("select * from normal where column3 like 'column31%'"))
    checkAnswer(
      sql("select * from colgrp where column3 not like 'column31%'"),
      sql("select * from normal where column3 not like 'column31%'"))
  }
  test("RowFilter") {
    checkAnswer(
      sql("select * from colgrp where column3 != column4"),
      sql("select * from normal where column3 != column4"))
  }

  test("Column Group not in order with schema") {
      //Add column group in order different then schema
    try {
      sql("create table colgrp_disorder (column1 string,column2 string,column3 string,column4 string,column5 string,column6 string,column7 string,column8 string,column9 string,column10 string,measure1 int,measure2 int,measure3 int,measure4 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES (\"COLUMN_GROUPS\"=\"(column7,column8),(column2,column3,column4)\")")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/10dim_4msr.csv' INTO table colgrp_disorder options('FILEHEADER'='column1,column2,column3,column4,column5,column6,column7,column8,column9,column10,measure1,measure2,measure3,measure4')");
      assert(true)
    } catch {
      case ex: Exception => assert(false)
    }

  }
  override def afterAll {
    sql("drop table colgrp")
    sql("drop table normal")
    sql("drop table colgrp_dictexclude_before")
    sql("drop table colgrp_dictexclude_after")
    sql("drop table if exists colgrp_disorder")
  }
}
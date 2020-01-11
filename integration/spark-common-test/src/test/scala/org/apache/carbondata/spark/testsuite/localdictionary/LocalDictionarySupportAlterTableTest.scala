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

package org.apache.carbondata.spark.testsuite.localdictionary

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class LocalDictionarySupportAlterTableTest extends QueryTest with BeforeAndAfterAll{

  override protected def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS LOCAL1")
  }

  test("test alter table add column") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','local_dictionary_include'='city','no_inverted_index'='name')
      """.stripMargin)
    sql("alter table local1 add columns (alt string) tblproperties('local_dictionary_include'='alt')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city,alt"))
      case None => assert(false)
    }
  }

  test("test alter table add column default configs for local dictionary") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','no_inverted_index'='name')
      """.stripMargin)
    sql("alter table local1 add columns (alt string)")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,city,alt"))
      case None => assert(false)
    }
  }

  test("test alter table add column where duplicate columns present in local dictionary include") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','local_dictionary_include'='city','no_inverted_index'='name')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string) tblproperties('local_dictionary_include'='alt,alt')")
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE contains Duplicate Columns: alt. " +
        "Please check the DDL."))
  }

  test("test alter table add column where duplicate columns present in local dictionary include/exclude") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','local_dictionary_include'='city',
        | 'no_inverted_index'='name')
      """.stripMargin)
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string) tblproperties" +
        "('local_dictionary_include'='abc')")
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc does not exist in table. " +
        "Please check the DDL."))
    val exception2 = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string) tblproperties" +
        "('local_dictionary_exclude'='abc')")
    }
    assert(exception2.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc does not exist in table. " +
        "Please check the DDL."))
  }

  test("test alter table add column for datatype validation") {
    sql("drop table if exists local1")
    sql(
      """ | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_include'='city', 'no_inverted_index'='name')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string,abc int) tblproperties" +
        "('local_dictionary_include'='abc')")
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc is not a string/complex/varchar " +
        "datatype column. LOCAL_DICTIONARY_COLUMN should be no dictionary string/complex/varchar datatype" +
        " column.Please check the DDL."))
  }

  test("test alter table add column where duplicate columns are present in local dictionary include and exclude") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_include'='city', 'no_inverted_index'='name')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string,abc string) tblproperties" +
        "('local_dictionary_include'='abc','local_dictionary_exclude'='alt,abc')")
    }
    assert(exception.getMessage
      .contains(
        "Column ambiguity as duplicate column(s):abc is present in LOCAL_DICTIONARY_INCLUDE " +
        "and LOCAL_DICTIONARY_EXCLUDE. Duplicate columns are not allowed."))
  }

  test("test alter table add column unsupported table property") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_include'='city', 'no_inverted_index'='name')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string,abc string) tblproperties" +
        "('local_dictionary_enable'='abc')")
    }
    assert(exception.getMessage
      .contains(
        "Unsupported Table property in add column: local_dictionary_enable"))
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string,abc string) tblproperties" +
        "('local_dictionary_threshold'='10000')")
    }
    assert(exception1.getMessage
      .contains(
        "Unsupported Table property in add column: local_dictionary_threshold"))
  }

  test("test alter table add column when main table is disabled for local dictionary") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='false',
        | 'local_dictionary_include'='city', 'no_inverted_index'='name')
      """.stripMargin)
    sql(
      "alter table local1 add columns (alt string,abc string) tblproperties" +
      "('local_dictionary_include'='abc')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
      case None => assert(false)
    }

    checkExistence(sql("DESC FORMATTED local1"), false,
      "Local Dictionary Include")
  }

  test("test local dictionary threshold for boundary values") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='300000')
      """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
      case None => assert(false)
    }
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='500')
      """.stripMargin)
    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
      case None => assert(false)
    }
  }

  test("test alter table add column for local dictionary include and exclude configs") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_include'='city', 'no_inverted_index'='name')
      """.stripMargin)
    sql(
      "alter table local1 add columns (alt string,abc string) tblproperties" +
      "('local_dictionary_include'='abc','local_dictionary_exclude'='alt')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city,abc"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("alt"))
      case None => assert(false)
    }
  }

  test("test local dictionary for varchar datatype columns") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_include'='city',
        | 'LONG_STRING_COLUMNS'='city')
      """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
      case None => assert(false)
    }
  }

  test("test local dictionary describe formatted only with default configs") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,city"))
      case None => assert(false)
    }
  }

  test("test local dictionary for invalid threshold") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='300000')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
      case None => assert(false)
    }
  }

  test("test alter set for local dictionary enable to disable") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='300000')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
      case None => assert(false)
    }
    sql("alter table local1 set tblproperties('local_dictionary_enable'='false')")
    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
      case None => assert(false)
    }
    checkExistence(sql("DESC FORMATTED local1"), false,
      "Local Dictionary Threshold")
  }

  test("test alter set for local dictionary _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    sql("alter table local1 set tblproperties('local_dictionary_threshold'='30000')")
    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("30000"))
      case None => assert(false)
    }
  }


  test("test alter set for local dictionary _002") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    sql("alter table local1 set tblproperties('local_dictionary_include'='name')")
    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name") && !row.get(1).toString.contains("city"))
      case None => assert(false)
    }
  }

  test("test alter set for local dictionary _003") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='name')")
    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
      case None => assert(false)
    }
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
      case None => assert(false)
    }
  }

  test("test alter set for local dictionary _004") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    sql("alter table local1 set tblproperties('local_dictionary_include'='city')")

    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
      case None => assert(false)
    }
  }

  test("test alter set for local dictionary _005") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    sql("alter table local1 set tblproperties('local_dictionary_include'='name,city')")

    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,city"))
      case None => assert(false)
    }
  }

  test("test alter set for local dictionary _006") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='city')")

    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
      case None => assert(false)
    }
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
      case None => assert(false)
    }
  }

  test("test alter set for local dictionary _007") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    val exception1 = intercept[Exception] {
      sql("alter table local1 set tblproperties('local_dictionary_include'='city, ')")
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column:  does not exist in table. " +
        "Please check the DDL."))
  }

  test("test alter set for local dictionary _008") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    val exception1 = intercept[Exception] {
      sql("alter table local1 set tblproperties('local_dictionary_exclude'='name')")
    }
    assert(exception1.getMessage.contains("Column ambiguity as duplicate column(s):name is present in LOCAL_DICTIONARY_INCLUDE and LOCAL_DICTIONARY_EXCLUDE. Duplicate columns are not allowed."))
  }

  test("test alter set for local dictionary _009") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    val exception1 = intercept[Exception] {
      sql("alter table local1 set tblproperties('local_dictionary_exclude'='id')")
    }
    assert(exception1.getMessage.contains("LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: id is not a string/complex/varchar datatype column. LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE should be no dictionary string/complex/varchar datatype column."))
  }

  test("test alter set for local dictionary _011") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='name')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='city')")
    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
      case None => assert(false)
    }
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
      case None => assert(false)
    }
  }

  test("test alter set for local dictionary _012") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='name')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    val exception1 = intercept[Exception] {
      sql("alter table local1 set tblproperties('local_dictionary_exclude'='city, ')")
    }
    assert(exception1.getMessage.contains("LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column:  does not exist in table. Please check the DDL."))
  }

  test("test alter set for local dictionary _014") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    val exception1 = intercept[Exception] {
      sql("alter table local1 set tblproperties('local_dictionary_exclude'='name')")
    }
    assert(exception1.getMessage.contains("Column ambiguity as duplicate column(s):name is present in LOCAL_DICTIONARY_INCLUDE and LOCAL_DICTIONARY_EXCLUDE. Duplicate columns are not allowed."))
  }

  test("test alter set for local dictionary _015") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    sql("alter table local1 set tblproperties('local_dictionary_enable'='false')")
    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
      case None => assert(false)
    }
  }

  test("test alter set for local dictionary _016") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='false')
      """.stripMargin)

    sql("alter table local1 set tblproperties('local_dictionary_include'='name')")
    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
      case None => assert(false)
    }
  }

  test("test alter set for local dictionary _017") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_include'='name','local_dictionary_exclude'='city')
      """.stripMargin)

    val exception1 = intercept[Exception] {
      sql("alter table local1 set tblproperties('local_dictionary_exclude'='name')")
    }
    assert(exception1.getMessage.contains("Column ambiguity as duplicate column(s):name is present in LOCAL_DICTIONARY_INCLUDE and LOCAL_DICTIONARY_EXCLUDE. Duplicate columns are not allowed."))
  }

  test("test alter unset for local dictionary") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name','local_dictionary_exclude'='city')
      """.stripMargin)

    sql("alter table local1 unset tblproperties('local_dictionary_exclude')")
    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
      case None => assert(false)
    }
  }

  test("test alter set for local dictionary disable to enable") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='300000')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
      case None => assert(false)
    }
    checkExistence(sql("DESC FORMATTED local1"), false,
      "Local Dictionary Threshold")
    sql("alter table local1 set tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='30000')")
    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("30000"))
      case None => assert(false)
    }
  }

  test("test alter set same column for local dictionary exclude and include") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_exclude'='city')
      """.stripMargin)
    intercept[Exception] {
      sql(
        "alter table local1 set tblproperties('local_dictionary_include'='name'," +
        "'local_dictionary_exclude'='name')")
    }
  }

  test("test alter set for valid and invalid complex type as include/exclude") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,st struct<s_id:int,
        | s_name:string,s_city:array<string>>, dcity array<string>)
        | STORED AS carbondata
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city,dcity',
        | 'local_dictionary_enable'='true')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(
        row.get(1).toString.contains("city,dcity") && !row.get(1).toString.contains("name"))
      case None => assert(false)
    }
    intercept[Exception] {
      sql("alter table local1 set tblproperties('local_dictionary_exclude'='dcity')")
    }
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='st')")
    val descFormatted2 = sql("describe formatted local1").collect
    descFormatted2.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("st"))
      case None => assert(false)
    }
    descFormatted2.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city,dcity"))
      case None => assert(false)
    }
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='st'," +
        "'local_dictionary_include'='dcity')")
    val descFormatted3 = sql("describe formatted local1").collect
    descFormatted3.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("st"))
      case None => assert(false)
    }
    descFormatted3.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("dcity"))
      case None => assert(false)
    }
  }

  test("test alter set for invalid complex type as include/exclude") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,st struct<s_id:int,
        | s_name:int,s_city:array<int>>, dcity array<int>)
        | STORED AS carbondata
        | tblproperties('local_dictionary_exclude'='name',
        | 'local_dictionary_enable'='true')
      """.
        stripMargin)
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='dcity')")

    intercept[Exception] {
      sql("alter table local1 set tblproperties('local_dictionary_include'='st')")
    }
  }

  test("test alter unset for local dictionary disable") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,add string)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='false')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Enable")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
      case None => assert(false)
    }
    sql("alter table local1 unset tblproperties('local_dictionary_enable')")
    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Enable")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
  }

  test("test alter unset for local dictionary enable local dict include") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,add string)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_include'='city')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
      case None => assert(false)
    }
    sql("alter table local1 unset tblproperties('local_dictionary_include')")
    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,city,add"))
      case None => assert(false)
    }
  }

  test("test alter unset for local dictionary enable local dict exclude") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,add string)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_include'='city',
        | 'local_dictionary_exclude'='name')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
      case None => assert(false)
    }
    sql("alter table local1 unset tblproperties('local_dictionary_exclude')")
    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
      case None => assert(false)
    }
  }

  test("test alter unset for valid/invalid complex type as include/exclude") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,st struct<s_id:int,
        | s_name:string,s_city:array<int>>, dcity array<string>)
        | STORED AS carbondata
        | tblproperties('local_dictionary_exclude'='st','local_dictionary_include'='city',
        | 'local_dictionary_enable'='true')
      """.
        stripMargin)
    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
      case None => assert(false)
    }
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(
        row.get(1).toString.contains("st.s_name") && !row.get(1).toString.contains("st.s_id"))
      case None => assert(false)
    }
    sql("alter table local1 unset tblproperties('local_dictionary_exclude')")
    sql("alter table local1 unset tblproperties('local_dictionary_include')")
    val descLoc2 = sql("describe formatted local1").collect
    descLoc2.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,city,st.s_name,dcity.val"))
      case None => assert(false)
    }
  }

  test("test alter table drop column for local dictionary include") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name struct<n:int,m:string>, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','no_inverted_index'='name',
        | 'local_dictionary_include'='name,city')
      """.stripMargin)
    sql("alter table local1 drop columns (city)")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name") && !row.get(1).toString.contains("city"))
      case None => assert(false)
    }
  }

  test("test alter table drop column for local dictionary exclude") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','no_inverted_index'='name',
        | 'local_dictionary_exclude'='name,city')
      """.stripMargin)
    sql("alter table local1 drop columns (name)")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("city") && !row.get(1).toString.contains("name"))
      case None => assert(false)
    }
  }

  test("test alter set case sensitivity") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','no_inverted_index'='name',
        | 'local_dictionary_include'='city')
      """.stripMargin)
    intercept[Exception] {
      sql("alter table local1 set tblproperties('local_dictionary_include'='city, CiTy')")
    }
    intercept[Exception] {
      sql("alter table local1 set tblproperties('local_dictionary_include'='naMe , NaMe')")
    }
  }

  test("test alter add and drop columns") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','no_inverted_index'='name')
      """.stripMargin)
    sql("alter table local1 add columns(add1 string,add2 string) tblproperties('local_dictionary_exclude'='add1')")
    sql("alter table local1 drop columns (add1)")
    sql("alter table local1 add columns(add1 string) tblproperties('local_dictionary_include'='add1')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,city,add2,add1"))
      case None => assert(false)
    }
  }

  test("test alter add columns and set properties") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','no_inverted_index'='name')
      """.stripMargin)
    sql("alter table local1 add columns(add1 string) tblproperties('local_dictionary_include'='add1')")
    sql("alter table local1 set tblproperties('local_dictionary_include'='name')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name") && !row.get(1).toString.contains("city,add2,add1"))
      case None => assert(false)
    }
  }

  test("test alter add columns and set properties _002") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','no_inverted_index'='name')
      """.stripMargin)
    sql("alter table local1 add columns(add1 string) tblproperties('local_dictionary_include'='add1')")
    sql("alter table local1 set tblproperties('local_dictionary_enable'='false')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
      case None => assert(false)
    }
  }

  test("test alter set on complex columns __001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,st struct<s_id:int,
        | s_name:string,s_city:array<string>>, dcity array<string>)
        | STORED AS carbondata
        | tblproperties('local_dictionary_enable'='true')
      """.
        stripMargin)
    sql("alter table local1 set tblproperties('local_dictionary_include'='st')")
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(
        row.get(1).toString.contains("st.s_name,st.s_city.val") &&
        !row.get(1).toString.contains("dcity"))
      case None => assert(false)
    }
    sql("alter table local1 unset tblproperties('local_dictionary_include')")
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='st')")
    val descFormatted2 = sql("describe formatted local1").collect
    descFormatted2.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("st"))
      case None => assert(false)
    }
    descFormatted2.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city,dcity"))
      case None => assert(false)
    }
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='st'," +
        "'local_dictionary_include'='dcity')")
    val descFormatted3 = sql("describe formatted local1").collect
    descFormatted3.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("st"))
      case None => assert(false)
    }
    descFormatted3.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("dcity"))
      case None => assert(false)
    }
  }

  test("test alter set on complex columns __002") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,st struct<s_id:int,
        | s_name:string,s_city:array<string>>, dcity array<string>)
        | STORED AS carbondata
        | tblproperties('local_dictionary_include'='st','local_dictionary_enable'='true')
      """.
        stripMargin)
    sql("alter table local1 set tblproperties('local_dictionary_include'='dcity')")

    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(
        row.get(1).toString.contains("dcity.val") &&
        !row.get(1).toString.contains("st"))
      case None => assert(false)
    }
  }

  test("test alter set on complex columns __003") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,st struct<s_id:int,
        | s_city:array<string>,s_name:string>, dcity array<string>)
        | STORED AS carbondata
        | tblproperties('local_dictionary_include'='st','local_dictionary_enable'='true')
      """.
        stripMargin)
    sql("alter table local1 unset tblproperties('local_dictionary_include')")
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='st')")
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(
        row.get(1).toString.contains("name,city,dcity.val") &&
        !row.get(1).toString.contains("st"))
      case None => assert(false)
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(
        row.get(1).toString.contains("st.s_city.val,st.s_name"))
      case None => assert(false)
    }
  }

  test("test alter set on complex columns __004") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,st struct<s_id:int,
        | s_city:array<string>,s_name:string>, dcity array<string>)
        | STORED AS carbondata
        | tblproperties('local_dictionary_include'='st,city,name','local_dictionary_exclude'='dcity','local_dictionary_enable'='true')
      """.
        stripMargin)
    sql("alter table local1 unset tblproperties('local_dictionary_exclude')")
    sql("alter table local1 set tblproperties('local_dictionary_include'='dcity')")
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(
        row.get(1).toString.contains("dcity.val"))
      case None => assert(false)
    }
  }

  test("test alter for local dictionary with custom configurations when varchar columns are specified _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string,city string, st array<struct<si:string,sd:int>>)
        | STORED AS carbondata
        | tblproperties('long_string_columns'='name','local_dictionary_enable'='true')
      """.stripMargin)
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='st,name')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,st.val.si"))
      case None => assert(false)
    }
  }

  test("test alter for local dictionary with custom configurations when varchar columns are specified _002") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string,city string, st array<struct<si:string,sd:int>>)
        | STORED AS carbondata
        | tblproperties('long_string_columns'='name','local_dictionary_enable'='true',
        | 'local_dictionary_include'='name')
      """.stripMargin)
    sql("alter table local1 set tblproperties('local_dictionary_include'='st,name')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,st.val.si"))
      case None => assert(false)
    }
  }

  test("test alter for local dictionary for complex columns when local dictionary exclude is defined _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string,city string, st array<struct<si:int,sd:string>>)
        | STORED AS carbondata
        | tblproperties('long_string_columns'='name','local_dictionary_enable'='true')
      """.stripMargin)
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='st,name')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,st.val.sd"))
      case None => assert(false)
    }
  }

  test("test alter for local dictionary for complex columns when local dictionary exclude is defined _002") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string,city string, st array<struct<si:int,sd:string>>,f string,g int,h string)
        | STORED AS carbondata
        | tblproperties('long_string_columns'='name','local_dictionary_enable'='true','local_dictionary_include'='st')
      """.stripMargin)
    sql("alter table local1 unset tblproperties('local_dictionary_include')")
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='st,name,h')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("h,name,st.val.sd"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
            case Some(row) => assert(row.get(1).toString.contains("city,f"))
            case None => assert(false)
    }
  }

  test("test alter for local dictionary for complex columns when local dictionary exclude is defined _003") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string,city string, st struct<si:int,sd:string,sh:array<string>>,f string,g int,h string)
        | STORED AS carbondata
        | tblproperties('long_string_columns'='name','local_dictionary_enable'='true','local_dictionary_include'='st')
      """.stripMargin)
    sql("alter table local1 unset tblproperties('local_dictionary_include')")
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='st,name,h')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("h,name,st.sd,st.sh.val"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city,f"))
      case None => assert(false)
    }
  }

  test("test alter for local dictionary for complex columns when local dictionary exclude is defined _004") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string,city string, st struct<si:int,sd:string>,f string,g int,h string)
        | STORED AS carbondata
        | tblproperties('long_string_columns'='name','local_dictionary_enable'='true','local_dictionary_include'='st')
      """.stripMargin)
    sql("alter table local1 unset tblproperties('local_dictionary_include')")
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='st,name,h')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("h,name,st.sd"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city,f"))
      case None => assert(false)
    }
  }

  test("test alter for local dictionary for complex columns when local dictionary exclude is defined _005") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string,city string, st array<string>,g string,f int,h string)
        | STORED AS carbondata
        | tblproperties('long_string_columns'='name','local_dictionary_enable'='true','local_dictionary_include'='st')
      """.stripMargin)
    sql("alter table local1 unset tblproperties('local_dictionary_include')")
    sql("alter table local1 set tblproperties('local_dictionary_exclude'='st,name,h')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("h,name,st.val"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city,g"))
      case None => assert(false)
    }
  }

  test("test alter table add column system level property and table level property") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.LOCAL_DICTIONARY_SYSTEM_ENABLE, "false")
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','local_dictionary_include'='city','no_inverted_index'='name')
      """.stripMargin)
    sql("alter table local1 add columns (alt string) tblproperties('local_dictionary_include'='alt')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city,alt"))
      case None => assert(false)
    }
  }

  test("test alter table add column system level property") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.LOCAL_DICTIONARY_SYSTEM_ENABLE, "false")
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED AS carbondata tblproperties
        | ('local_dictionary_threshold'='20000','local_dictionary_include'='city',
        | 'no_inverted_index'='name')
      """.stripMargin)
    // exception will not be thrown as validation is not done, because table level local
    // dictionary property is not configured, but system level it is configured false
    sql(
      "alter table local1 add columns (alt int)")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enable")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
      case None => assert(false)
    }
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS LOCAL1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.LOCAL_DICTIONARY_SYSTEM_ENABLE,
        CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT)
  }
}

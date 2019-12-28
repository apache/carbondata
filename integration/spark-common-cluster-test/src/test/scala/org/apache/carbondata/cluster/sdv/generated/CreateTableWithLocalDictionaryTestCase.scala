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

package org.apache.carbondata.cluster.sdv.generated

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

class CreateTableWithLocalDictionaryTestCase extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS LOCAL1")
  }

  test("test local dictionary default configuration") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    val desc_result = sql("describe formatted local1")

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations for local dict columns _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations for local dict columns _003") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column:  does not exist in table. Please check " +
        "the DDL."))
  }

  test("test local dictionary custom configurations for local dict columns _004") {
    sql("drop table if exists local1")
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='abc')
        """.
          stripMargin)
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc does not exist in table. Please check " +
        "the DDL."))
  }

  test("test local dictionary custom configurations for local dict columns _005") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='id')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: id is not a string/complex/varchar datatype column. " +
        "LOCAL_DICTIONARY_COLUMN should " +
        "be no dictionary string/complex/varchar datatype column"))
  }

  test("test local dictionary custom configurations for local dict columns _006") {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name')
        """.
          stripMargin)
    }
  }

  test("test local dictionary custom configurations for local dict threshold _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
  }

  test("test local dictionary custom configurations for local dict threshold _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='-100')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations for local dict threshold _003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='21474874811')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations for local dict threshold _004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations for local dict threshold _005")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='hello')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_001")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='-100','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='vdslv','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_005")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000','local_dictionary_include'='name,name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_006")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000','local_dictionary_include'=' ')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_007")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000','local_dictionary_include'='hello')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_008")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000','local_dictionary_include'='name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_009")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='','local_dictionary_include'='name,name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_010")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='-100','local_dictionary_include'='Hello')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_011")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='23213497321591234324',
          | 'local_dictionary_include'='name')
        """.stripMargin)
    }
  }

  test("test local dictionary default configuration when enabled") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true')
      """.stripMargin)

    val desc_result = sql("describe formatted local1")

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict columns _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_include'='name','local_dictionary_enable'='true')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict columns _002")
  {
    sql("drop table if exists local1")

    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations when enabled for local dict columns _003") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column:  does not exist in table. Please check " +
        "the DDL."))

  }

  test("test local dictionary custom configurations when enabled for local dict columns _004") {
    sql("drop table if exists local1")
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='abc')
        """.
          stripMargin)
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc does not exist in table. Please check " +
        "the DDL."))
  }

  test("test local dictionary custom configurations when enabled for local dict columns _005") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='id')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: id is not a string/complex/varchar datatype column. " +
        "LOCAL_DICTIONARY_COLUMN should " +
        "be no dictionary string/complex/varchar datatype column"))
  }

  test("test local dictionary custom configurations when enabled for local dict columns _006") {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true',
          | 'local_dictionary_include'='name')
        """.
          stripMargin)
    }
  }

  test("test local dictionary custom configurations when local_dictionary_exclude is configured _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_enable'='true')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations when local_dictionary_exclude is configured _002")
  {
    sql("drop table if exists local1")

    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='name,name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations when local_dictionary_exclude is configured _003") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column:  does not exist in table. Please check " +
        "the DDL."))

  }

  test("test local dictionary custom configurations when local_dictionary_exclude is configured _004") {
    sql("drop table if exists local1")
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='abc')
        """.
          stripMargin)
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc does not exist in table. Please check " +
        "the DDL."))
  }

  test("test local dictionary custom configurations when local_dictionary_exclude is configured _005") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='id')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: id is not a string/complex/varchar datatype column. " +
        "LOCAL_DICTIONARY_COLUMN should " +
        "be no dictionary string/complex/varchar datatype column"))
  }

  test("test local dictionary custom configurations when local_dictionary_exclude is configured _006") {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true',
          | 'local_dictionary_exclude'='name')
        """.
          stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when local_dictionary_include and local_dictionary_exclude " +
    "is configured _001")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city',
        | 'local_dictionary_enable'='true')
      """.
        stripMargin)

    val descFormatted1 = sql("describe formatted local1").collect

    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
    }
  }

  test(
    "test local dictionary custom configurations when local_dictionary_include and local_dictionary_exclude " +
    "is configured _002") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,add string)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city','sort_columns'='add',
        | 'local_dictionary_enable'='true')
      """.
        stripMargin)

    val descFormatted1 = sql("describe formatted local1").collect

    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
    }
  }

  test(
    "test local dictionary custom configurations when local_dictionary_include and local_dictionary_exclude " +
    "is configured _003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city',
        | 'local_dictionary_enable'='false')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }

    checkExistence(sql("describe formatted local1"), false, "Local Dictionary Include")
    checkExistence(sql("describe formatted local1"), false, "Local Dictionary Exclude")
  }

  test(
    "test local dictionary custom configurations when local_dictionary_include and local_dictionary_exclude " +
    "is configured _004")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city',
          | 'local_dictionary_enable'='true')
        """.
          stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when local_dictionary_include and local_dictionary_exclude " +
    "is configured _005")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,city',
          | 'local_dictionary_exclude'='name')
        """.
          stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when local_dictionary_include and local_dictionary_exclude " +
    "is configured _006")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,st struct<s_id:int,
        | s_name:string,s_city:array<string>>)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city,st',
        | 'local_dictionary_enable'='true')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city,st"))
    }
  }

  test(
    "test local dictionary custom configurations when local_dictionary_include and local_dictionary_exclude " +
    "is configured _007")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,st array<struct<s_id:int,
        | s_name:string>>)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city,st',
        | 'local_dictionary_enable'='true')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city,st"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict threshold _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict threshold _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='-100')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict threshold _003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='21474874811')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict threshold _004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict threshold _005")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='hello')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _001")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='-100',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='vdslv',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _005")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000',
          | 'local_dictionary_include'='name,name')
        """.stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _006")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000',
          | 'local_dictionary_include'=' ')
        """.stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _007")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000',
          | 'local_dictionary_include'='hello')
        """.stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _008")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000',
          | 'local_dictionary_include'='name' )
        """.stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _009")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='',
          | 'local_dictionary_include'='name,name')
        """.stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _010")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='-100',
          | 'local_dictionary_include'='Hello')
        """.stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _011")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true',
          | 'local_dictionary_threshold'='23213497321591234324','local_dictionary_include'='name')
        """.stripMargin)
    }
  }

  test("test local dictionary default configuration when disabled") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)

    val desc_result = sql("describe formatted local1")

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_include'='name','local_dictionary_enable'='false')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_include'='name,name')
      """.stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _003") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_include'='')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _004") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_include'='abc')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _005") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_include'='id')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _006") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false' ,
        | 'local_dictionary_include'='name')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict threshold _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='20000')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict threshold _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='-100')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict threshold _003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='21474874811')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict threshold _004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict threshold _005")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='hello')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _001")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='20000',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='-100',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='vdslv',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _005")
  {
    sql("drop table if exists local1")

    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='20000',
        | 'local_dictionary_include'='name,name')
      """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _006")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='20000',
        | 'local_dictionary_include'=' ')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _007")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='20000',
        | 'local_dictionary_include'='hello')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _008")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='20000',
        | 'local_dictionary_include'='name' )
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _009")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='',
        | 'local_dictionary_include'='name,name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _010")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='-100',
        | 'local_dictionary_include'='Hello')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _011")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false',
        | 'local_dictionary_threshold'='23213497321591234324','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect

    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configuration with other table properties _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties(
        | 'sort_scope'='global_sort',
        | 'sort_columns'='city,name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect

    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
    descLoc.find(_.get(0).toString.contains("Sort Scope")) match {
      case Some(row) => assert(row.get(1).toString.contains("global_sort"))
    }
  }

  test("test local dictionary custom configuration with other table properties _002") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('sort_scope'='local_sort',
        | 'sort_columns'='city,name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect

    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
    descLoc.find(_.get(0).toString.contains("Sort Scope")) match {
      case Some(row) => assert(row.get(1).toString.contains("local_sort"))
    }
  }
  test("test local dictionary custom configuration with other table properties _003") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties(
        | 'sort_scope'='no_sort',
        | 'sort_columns'='city,name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect

    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
    descLoc.find(_.get(0).toString.contains("Sort Scope")) match {
      case Some(row) => assert(row.get(1).toString.contains("no_sort"))
    }
  }
  test("test local dictionary custom configuration with other table properties _004") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties(
        | 'sort_scope'='local_sort',
        | 'sort_columns'='city,name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect

    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
    descLoc.find(_.get(0).toString.contains("Sort Scope")) match {
      case Some(row) => assert(row.get(1).toString.contains("local_sort"))
    }
  }

  test("test CTAS statements for local dictionary default configuration when enabled") {
    sql("drop table if exists local")
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | create table local1 stored by 'carbondata' tblproperties('local_dictionary_enable'='true') as
        | select * from local
      """.stripMargin)

    val desc_result = sql("describe formatted local1")

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test CTAS statements for local dictionary custom configurations when enabled for local dict columns _001") {
    sql("drop table if exists local")
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_include'='name','local_dictionary_enable'='true')
        | as select * from local
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test CTAS statements for local dictionary custom configurations when enabled for local dict columns _002")
  {
    sql("drop table if exists local")
    sql("drop table if exists local1")

    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,name')
          | as select * from local
        """.stripMargin)
    }
  }

  test("test CTAS statements for local dictionary custom configurations when enabled for local dict columns _003") {
    sql("drop table if exists local")
    sql("drop table if exists local1")

    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='')
          | as select * from local
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column:  does not exist in table. Please check " +
        "the DDL."))

  }

  test("test CTAS statements for local dictionary custom configurations when enabled for local dict columns _004") {
    sql("drop table if exists local")
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='abc')
          | as select * from local
        """.
          stripMargin)
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc does not exist in table. Please check " +
        "the DDL."))
  }

  test("test CTAS statements for local dictionary custom configurations when enabled for local dict columns _005") {
    sql("drop table if exists local")
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='id')
          | as select * from local
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: id is not a string/complex/varchar datatype column. " +
        "LOCAL_DICTIONARY_COLUMN should " +
        "be no dictionary string/complex/varchar datatype column"))
  }

  test("test CTAS statements for local dictionary custom configurations when enabled for local dict columns _006") {
    sql("drop table if exists local")
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true' ,
          | 'local_dictionary_include'='name') as select * from local
        """.
          stripMargin)
    }
  }

  test("test CTAS statements for local dictionary custom configurations when local_dictionary_exclude is configured _001") {
    sql("drop table if exists local")
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_enable'='true')
        | as select * from local
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test CTAS statements for local dictionary custom configurations when local_dictionary_exclude is configured _002")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='name,name')
          | as select * from local
        """.stripMargin)
    }
  }

  test("test CTAS statements for local dictionary custom configurations when local_dictionary_exclude is configured _003") {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='')
          | as select * from local
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column:  does not exist in table. Please check " +
        "the DDL."))
  }

  test("test CTAS statements for local dictionary custom configurations when local_dictionary_exclude is configured _004") {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='abc')
          | as select * from local
        """.
          stripMargin)
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc does not exist in table. Please check " +
        "the DDL."))
  }

  test("test CTAS statements for local dictionary custom configurations when local_dictionary_exclude is configured _005") {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='id')
          | as select * from local
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: id is not a string/complex/varchar datatype column. " +
        "LOCAL_DICTIONARY_COLUMN should " +
        "be no dictionary string/complex/varchar datatype column"))
  }

  test("test CTAS statements for local dictionary custom configurations when local_dictionary_exclude is configured _006") {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true' ,
          | 'local_dictionary_exclude'='name') as select * from local
        """.
          stripMargin)
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when local_dictionary_include and local_dictionary_exclude " +
    "is configured _001")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city',
        | 'local_dictionary_enable'='true') as select * from local
      """.
        stripMargin)

    val descFormatted1 = sql("describe formatted local1").collect

    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when local_dictionary_include and local_dictionary_exclude " +
    "is configured _002")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city',
        | 'local_dictionary_enable'='false') as select * from local
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }

    checkExistence(sql("describe formatted local1"), false, "Local Dictionary Include")
    checkExistence(sql("describe formatted local1"), false, "Local Dictionary Exclude")
  }

  test(
    "test CTAS statements for local dictionary custom configurations when local_dictionary_include and local_dictionary_exclude " +
    "is configured _003")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city',
          | 'local_dictionary_enable'='true') as select * from local
        """.
          stripMargin)
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when local_dictionary_include and local_dictionary_exclude " +
    "is configured _004")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,city',
          | 'local_dictionary_exclude'='name') as select * from local
        """.
          stripMargin)
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when local_dictionary_include and local_dictionary_exclude " +
    "is configured _005")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int,st struct<s_id:int,
        | s_name:string,s_city:array<string>>)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city,st',
        | 'local_dictionary_enable'='false') as select * from local
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test CTAS statements for local dictionary custom configurations when enabled for local dict threshold _001") {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000')
        | as select * from local
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
  }

  test("test CTAS statements for local dictionary custom configurations when enabled for local dict threshold _002")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='-100')
        | as select * from local
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test CTAS statements for local dictionary custom configurations when enabled for local dict threshold _003")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='23589714365172595')
        | as select * from local
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test CTAS statements for local dictionary custom configurations when first table is hive table")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        |  tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000','local_dictionary_include'='city')
        | as select * from local
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
    }
  }

  test("test no inverted index for local dictionary custom configurations when first table is hive table")
  {
    sql("drop table if exists local1")

    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','local_dictionary_include'='city','no_inverted_index'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
    }
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS LOCAL1")
  }
}
